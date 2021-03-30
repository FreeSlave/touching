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
import std.path : dirName;

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
    @optional @name("format_no_target_found") string formatNoTargetFound;
    @optional @name("parameters") string[][string] randomizedParams;
    @optional int chance;
    @optional @name("ignore_target") bool ignoreTarget;
}

struct AimedCommand
{
    @optional string format;
    @optional @name("format_self") string formatSelfTarget;
    @optional @name("format_no_target_found") string formatNoTargetFound;
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
    @optional @name("log_file") string logFile;
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

void fillRandomParameters(ref string[string] parameters, scope const string[][string] randomParams)
{
    foreach(string paramName, const(string)[] paramList; randomParams)
    {
        parameters[paramName] = paramList[uniform(0, paramList.length)];
    }
}

void handleAimedCommand(HTTPServerRequest req, HTTPServerResponse res, string commandName, scope const AimedCommand command, ServerContext context)
{
    enforceParameters(req);

    auto user = req.query["user"];
    auto pTarget = "target" in req.query;
    bool targetIsSelf = false;
    bool targetIsNotFound = false;
    string target = pTarget ? *pTarget : string.init;

    string format = command.format;
    string formatSelfTarget = command.formatSelfTarget;
    string formatNoTargetFound = command.formatNoTargetFound;
    string selfTarget = command.selfTargetName;

    string[string] parameters;
    fillRandomParameters(parameters, command.randomizedParams);

    if (command.variants.length)
    {
        size_t chooseTheVariantIndex() {
            import std.algorithm.searching : all;
            import std.algorithm.iteration : filter;
            import std.array;

            const(AimedCommandVariant)[] variants = command.variants;
            if (target.length > 0) {
                variants = variants.filter!(v => !v.ignoreTarget).array;
                if (variants.length == 0) {
                    variants = command.variants;
                }
            }

            if (all!"a.chance > 0"(variants))
            {
                int sum;
                foreach(ref v; variants) {
                    sum += v.chance;
                }
                int r = uniform(0, sum);
                int border;
                foreach(size_t i, ref v; variants)
                {
                    border += variants[i].chance;
                    if (r < border)
                        return i;
                }
                return 0;
            }
            else
            {
                return uniform(0, variants.length);
            }
        }

        auto chosenIndex = chooseTheVariantIndex();
        auto chosenVariant = command.variants[chosenIndex];

        if (chosenVariant.formatSelfTarget.length)
            formatSelfTarget = chosenVariant.formatSelfTarget;
        if (chosenVariant.formatNoTargetFound.length)
            formatNoTargetFound = chosenVariant.formatNoTargetFound;
        if (chosenVariant.format.length)
            format = chosenVariant.format;

        fillRandomParameters(parameters, command.variants[chosenIndex].randomizedParams);
    }

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
                else if (userIndex < users.length-1)
                    userIndex++;
                else
                    targetIsNotFound = true;
            }
            logInfo("user index: %s", userIndex);
            logInfo("user: %s", users[userIndex]);
            target = users[userIndex];
        }
    }

    if (targetIsUser(target))
    {
        targetIsSelf = true;
    }

    parameters["user"] = user;
    parameters["target"] = target;

    logInfo("user: '%s', command: '%s', target: '%s', self: %s, notarget: %s", user, commandName, target, targetIsSelf, targetIsNotFound);

    if (targetIsSelf || targetIsNotFound)
    {
        if (selfTarget.length)
            parameters["target"] = selfTarget;
        if (targetIsSelf)
        {
            if (formatSelfTarget.length)
                format = formatSelfTarget;
        }
        if (targetIsNotFound)
        {
            if (formatNoTargetFound.length)
                format = formatNoTargetFound;
            else if (formatSelfTarget.length)
                format = formatSelfTarget;
        }
    }
    plainTextAnswer(res, formatNamed(format, parameters));
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

    if (serverConfig.logFile.length) {
        setLogFile(serverConfig.logFile);
    }

    auto dataText = readFile(serverConfig.dataPath).assumeUnique.assumeUTF;
    auto context = new ServerContext(serverConfig, parseServerData(dataText));

    auto router = new URLRouter;
    router.get("/aimed_commands/:command", delegate(HTTPServerRequest req, HTTPServerResponse res) {
        auto commandName = req.params["command"];
        if (!commandName.length) {
            res.statusCode = HTTPStatus.Forbidden;
            plainTextAnswer(res, "Command must be specified");
        } else {
            auto command = commandName in context.data.aimedCommands;
            if (command is null)
            {
                res.statusCode = HTTPStatus.NotFound;
                plainTextAnswer(res, "Command "~commandName~" does not exist");
            }
            else
            {
                handleAimedCommand(req, res, commandName, *command, context);
            }
        }
    });

    auto httpSettings = new HTTPServerSettings;
    httpSettings.port = serverConfig.port;
    httpSettings.bindAddresses = ["0.0.0.0"];

    auto listenServer = listenHTTP(httpSettings, router);
    scope(exit) listenServer.stopListening();

    const dataDirName = serverConfig.dataPath.dirName;
    auto watcher = watchDirectory(dataDirName, false);

    auto timer = setTimer(dur!"seconds"(1), {
        DirectoryChange[] changes;
        if (watcher.readChanges(changes, dur!"seconds"(-1)))
        {
            auto dataNativePath = NativePath(serverConfig.dataPath);
            bool dataChanged = false;
            foreach(ref change; changes)
            {
                if (change.type == DirectoryChangeType.modified && change.path == dataNativePath)
                {
                    dataChanged = true;
                    break;
                }
            }

            if (dataChanged)
            {
                logInfo("%s changed. Trying to reload server data...", serverConfig.dataPath);
                try {
                    auto serverData = readFile(serverConfig.dataPath).assumeUnique.assumeUTF.parseServerData;
                    context.data = serverData;
                    logInfo("Successfully updated server data");
                } catch(Exception e) {
                    logError("Failed to load new data: %s", e.msg);
                }
            }
        }
    }, true);
    scope(exit) timer.stop();

    runApplication();
}
