import core.time;
import vibe.core.file;
import vibe.core.log;
import vibe.core.task;
import vibe.core.core;
import vibe.core.net;
import vibe.http.router;
import vibe.http.server;
import vibe.http.client;
import vibe.http.fileserver;
import vibe.data.json;
import vibe.utils.string;

import std.algorithm.sorting : sort;
import std.datetime.stopwatch;
import std.exception : assumeUnique;
import std.random : uniform;
import std.range : assumeSorted, SortedRange;

@safe bool isPriviligedUserLevel(string userLevel) nothrow pure @nogc
{
    switch(userLevel)
    {
        case "owner":
        case "moderator":
        case "subscriber":
        case "twitch_vip":
            return true;
        default:
            return false;
    }
}

void enforceParameters(ref HTTPServerRequest req)
{
    enforceHTTP("user" in req.query, HTTPStatus.badRequest, "Missing user field");
    enforceHTTP("userlevel" in req.query, HTTPStatus.badRequest, "Missing userlevel field");
}

void plainTextAnswer(ref HTTPServerResponse res, string text)
{
    res.writeBody(text, "text/plain;charset=utf-8");
}

struct AimedCommandVariant
{
    @optional string format;
    @optional @name("format_self") string formatSelfTarget;
    @optional @name("parameters") string[][string] randomizedParams;
    @optional int chance;
}

struct AimedCommand
{
    @optional string format;
    @optional @name("format_self") string formatSelfTarget;
    @optional @name("format_notarget") string formatNoTarget;
    @optional @name("self") string selfTargetName;
    @optional @name("parameters") string[][string] randomizedParams;
    @optional AimedCommandVariant[] variants;
    @optional @name("random_self_aim") bool allowRandomSelfAim;
}

struct ServerData
{
    @optional string[] bots;
    @optional @name("aimed_commands") AimedCommand[string] aimedCommands;
}

ServerData parseServerData(string dataText)
{
    auto dataJson = parseJsonString(dataText);
    auto serverData = deserializeJson!ServerData(dataJson);
    serverData.bots.sort();
    return serverData;
}

struct ServerConfig
{
    @optional @name("data_path") string dataPath = "./data.json";
    @optional ushort port = 27080;
    @optional @name("refetch_users_interval") ushort refetchUsersInterval = 20;
    @name("twitch_channel") string twitchChannel;
}

ServerConfig parseServerConfig(string configText)
{
    auto configJson = parseJsonString(configText);
    auto serverConfig = deserializeJson!ServerConfig(configJson);
    serverConfig.twitchChannel = serverConfig.twitchChannel.toLower;
    return serverConfig;
}

string formatNamed(scope string format, scope const string[string] params)
{
    import std.array : appender;
    import std.ascii : isAlpha;
    import std.string : indexOf;

    auto result = appender!string;
    result.reserve(format.length);
    ptrdiff_t index = 0;
    size_t startIndex;
    do {
        startIndex = index;
        index = indexOf(format, '$', startIndex);
        if (index >= 0)
        {
            result.put(format[startIndex..index]);

            const startNameIndex = index;
            index++;
            while(index < format.length && (format[index] == '_' || isAlpha(format[index])))
            {
                index++;
            }
            const key = format[startNameIndex+1..index];
            const foundValue = key in params;
            if (foundValue)
            {
                result.put(*foundValue);
            }
            else
            {
                result.put(format[startNameIndex..index]);
            }
        }
        else
        {
            result.put(format[startIndex..$]);
        }
    }
    while(index >= 0);
    return result[];
}

unittest
{
    assert(formatNamed("hello $user, how is your $buisness?", ["user" : "Username", "buisness": "health"]) == "hello Username, how is your health?");
    assert(formatNamed("$user says hello to $target", ["user" : "User1", "target": "User2"]) == "User1 says hello to User2");

    // no expanding if there's no such key
    assert(formatNamed("$ says hello to $targetless", ["target": "no"]) == "$ says hello to $targetless");
    assert(formatNamed("", ["target":"no"]) == "");
}

struct AimedCommandHandler
{
    static void fillRandomParameters(ref string[string] parameters, scope const string[][string] randomParams)
    {
        foreach(string paramName, const(string)[] paramList; randomParams)
        {
            parameters[paramName] = paramList[uniform(0, paramList.length)];
        }
    }

    string name;
    const(AimedCommand) command;
    ServerContext context;

    this(string name, const(AimedCommand) command, ServerContext context)
    {
        this.name = name;
        this.command = command;
        this.context = context;
    }
    void opCall(HTTPServerRequest req, HTTPServerResponse res)
    {
        enforceParameters(req);

        auto user = req.query["user"];
        auto pTarget = "target" in req.query;
        bool targetIsSelf = false;
        bool targetIsNotFound = false;
        string target = pTarget ? *pTarget : string.init;

        bool targetIsUser(string target)
        {
            return icmp2(target, user) == 0 || (target.length > 1 && target[0] == '@' && icmp2(target[1..$], user) == 0);
        }

        if (target.length == 0)
        {
            auto users = context.recentUserList();
            if (users.length == 0)
                targetIsNotFound = true;
            else
            {
                auto userIndex = uniform(0, users.length);
                if (!command.allowRandomSelfAim && targetIsUser(users[userIndex]))
                {
                    if (userIndex > 0)
                        userIndex--;
                    else if (userIndex < users.length)
                        userIndex++;
                    else
                        targetIsNotFound = true;
                }
                target = users[userIndex];
            }
        }

        if (targetIsUser(target))
        {
            targetIsSelf = true;
        }

        string format = command.format;

        if (targetIsSelf || targetIsNotFound)
        {
            if (command.formatSelfTarget.length)
                format = command.formatSelfTarget;
            if (targetIsNotFound && command.formatNoTarget.length)
                format = command.formatNoTarget;
            if (command.selfTargetName.length)
                target = command.selfTargetName;
        }

        string[string] parameters;

        fillRandomParameters(parameters, command.randomizedParams);

        if (command.variants.length)
        {
            size_t chooseTheVariantIndex() {
                import std.algorithm.searching : all;
                if (all!"a.chance > 0"(command.variants))
                {
                    int sum;
                    foreach(ref v; command.variants) {
                        sum += v.chance;
                    }
                    int r = uniform(0, sum);
                    int border;
                    foreach(size_t i, ref v; command.variants)
                    {
                        border += command.variants[i].chance;
                        if (r < border)
                            return i;
                    }
                    return 0;
                }
                else
                {
                    return uniform(0, command.variants.length);
                }
            }

            auto chosenIndex = chooseTheVariantIndex();
            auto chosenVariant = command.variants[chosenIndex];

            if (targetIsSelf || targetIsNotFound)
            {
                if (chosenVariant.formatSelfTarget.length)
                    format = chosenVariant.formatSelfTarget;
            }
            else if (chosenVariant.format.length)
            {
                format = chosenVariant.format;
            }

            fillRandomParameters(parameters, command.variants[chosenIndex].randomizedParams);
        }

        parameters["user"] = user;
        parameters["target"] = target;

        logInfo("user: '%s', target: '%s', self: %s, notarget: %s", user, target, targetIsSelf, targetIsNotFound);

        plainTextAnswer(res, formatNamed(format, parameters));
    }
}

void setupAimedCommandRoutes(URLRouter router, ServerContext context)
{
    foreach(string name, ref command; context.data.aimedCommands)
    {
        auto t = AimedCommandHandler(name, command, context);
        router.get("/"~name, URLRouter.handlerDelegate(t));
    }
}

class ServerContext
{
    ServerConfig config;
    ServerData data;
    StopWatch usersUpdateWatch;
    string[] users;
    bool updatingUsers;

    this(ServerConfig config, ServerData data) {
        this.config = config;
        this.data = data;
        usersUpdateWatch = StopWatch(AutoStart.no);
    }

    void updateUserList()
    {
        HTTPClientResponse res;
        if (updatingUsers)
        {
            logInfo("Already in the process of updating users");
            return;
        }
        try {
            updatingUsers = true;
            res = requestHTTP("http://tmi.twitch.tv/group/user/" ~ config.twitchChannel ~ "/chatters", (scope HTTPClientRequest req) {});
            scope(exit) res.dropBody();
            auto chattersJson = res.readJson();

            import std.array : appender;

            auto chattersObj = chattersJson["chatters"];
            auto chatterCountJson = chattersJson["chatter_count"];
            auto chatterCount = chatterCountJson.get!size_t;

            auto allUsers = appender!(string[]);
            allUsers.reserve(chatterCount);
            allUsers.put(deserializeJson!(string[])(chattersObj["broadcaster"]));
            allUsers.put(deserializeJson!(string[])(chattersObj["vips"]));
            allUsers.put(deserializeJson!(string[])(chattersObj["moderators"]));
            allUsers.put(deserializeJson!(string[])(chattersObj["viewers"]));

            auto sortedBots = data.bots.assumeSorted;
            scope(exit) sortedBots.release();
            auto nonBotUsers = appender!(string[]);
            nonBotUsers.reserve(chatterCount);
            foreach(user; allUsers) {
                if (!sortedBots.contains(user)) {
                    nonBotUsers.put(user);
                }
            }
            users = nonBotUsers[];
            users.sort();
            logInfo("Updated users: %s", users);
        } catch(Exception e) {
            logError("Error during updating twitch user list: %s", e);
        } finally {
            updatingUsers = false;
        }
    }

    auto recentUserList() {
        if (!usersUpdateWatch.running) {
            logInfo("Fetching users for the first time");
            updateUserList();
            usersUpdateWatch.start();
        }
        if (usersUpdateWatch.peek.total!"seconds" >= config.refetchUsersInterval) {
            logInfo("Re-fetching users");
            updateUserList();
            usersUpdateWatch.reset();
        }
        return users.assumeSorted;
    }
}

void main()
{
    auto configText = readFile("./config.json").assumeUnique.assumeUTF;
    auto serverConfig = parseServerConfig(configText);

    auto dataText = readFile(serverConfig.dataPath).assumeUnique.assumeUTF;
    auto context = new ServerContext(serverConfig, parseServerData(dataText));

    auto router = new URLRouter;
    setupAimedCommandRoutes(router, context);

    auto httpSettings = new HTTPServerSettings;
    httpSettings.port = serverConfig.port;
    httpSettings.bindAddresses = ["0.0.0.0"];

    auto listenServer = listenHTTP(httpSettings, router);
    scope(exit) listenServer.stopListening();

    runApplication();
}
