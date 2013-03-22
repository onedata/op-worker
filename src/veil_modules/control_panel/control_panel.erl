-module(control_panel).
-behaviour(worker_plugin_behaviour).
-export([init/1,
        handle/2,
        cleanUp/0]).

%% ===================================================================
%% API functions
%% ===================================================================

init([{CCM_process_name, CCM_node_name}, {DAO_process_name, DAO_node_name}]) ->
    %% Start the Process Registry...
    application:start(nprocreg),

    %% Start Cowboy...
    application:start(cowboy),

    %% Usuniete z app.src -> env
    %% {bind_address, "0.0.0.0"},
    %% {port, 8000},
    %% {server_name, nitrogen},
    %% {document_root, "src/veil_modules/control_panel/static"},
    %% {static_paths, ["js/","images/","css/","nitrogen/","templates/"]}

    %% {ok, BindAddress} = application:get_env(bind_address),
    %% {ok, Port} = application:get_env(port),
    %% {ok, ServerName} = application:get_env(server_name),
    %% {ok, DocRoot} = application:get_env(document_root),
    %% {ok, StaticPaths} = application:get_env(static_paths),

    application:set_env(veil_cluster_node, ccm_address, {CCM_process_name, CCM_node_name}),
    application:set_env(veil_cluster_node, dao_address, {DAO_process_name, DAO_node_name}),


    BindAddress = "0.0.0.0",
    Port = 8000,
    ServerName = vfsgui,
    DocRoot = "src/veil_modules/control_panel/static",
    StaticPaths = ["js/","images/","css/","nitrogen/","templates/"],
    DocRootBin = wf:to_binary(DocRoot),

    io:format("Starting Cowboy Server (~s) on ~s:~p, root: '~s'~n",
              [ServerName, BindAddress, Port, DocRoot]),

    StaticDispatches = lists:map(fun(Dir) ->
        Path = reformat_path(Dir),
        Handler = cowboy_http_static,
        Opts = [
            {directory, filename:join([DocRootBin | Path])},
            {mimetypes, {fun mimetypes:path_to_mimes/2, default}}
        ],
        {Path ++ ['...'],Handler,Opts}
    end,StaticPaths),

    %% HandlerModule will end up calling HandlerModule:handle(Req,HandlerOpts)
    HandlerModule = cp_cowboy_handler,
    HandlerOpts = [],

    %% Start Cowboy...
    %% NOTE: According to Loic, there's no way to pass the buck back to cowboy
    %% to handle static dispatch files so we want to make sure that any large
    %% files get caught in general by cowboy and are never passed to the nitrogen
    %% handler at all. In general, your best bet is to include the directory in
    %% the static_paths section of cowboy.config
    Dispatch = [
        %% Nitrogen will handle everything that's not handled in the StaticDispatches
        {'_', StaticDispatches ++ [{'_',HandlerModule , HandlerOpts}]}
    ],
    HttpOpts = [{max_keepalive, 50}, {dispatch, Dispatch}],
    cowboy:start_listener(http, 100,
                          cowboy_tcp_transport, [{port, Port}],
                          cowboy_http_protocol, HttpOpts),

    {ok, { {one_for_one, 5, 10}, []} }.

handle(_ProtocolVersion, _Request) -> {ok, dummy}.

cleanUp() -> ok.


%% ===================================================================
%% Auxiliary functions
%% ===================================================================

%% If the path is a string, it's going to get split on / and switched to the binary
%% format that cowboy expects
reformat_path(Path) when is_list(Path) andalso is_integer(hd(Path)) ->
    TokenizedPath = string:tokens(Path,"/"),
    [list_to_binary(Part) || Part <- TokenizedPath];
%% If path is just a binary, let's make it a string and treat it as such
reformat_path(Path) when is_binary(Path) ->
    reformat_path(binary_to_list(Path));
%% If path is a list of binaries, then we assume it's formatted for cowboy format already
reformat_path(Path) when is_list(Path) andalso is_binary(hd(Path)) ->
    Path.
