%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour callbacks.
%% It is responsible for setting up a nitrogen website, running on cowboy server.
%% @end
%% ===================================================================

-module(control_panel).
-behaviour(worker_plugin_behaviour).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1 <br />
%% Starts required applications (nprocreg and cowboy), sets up
%% cowboy dispatch with nitrogen handler and finally starts
%% cowboy service on desired port.
%% @end
-spec init(Args :: term()) -> Result when
  Result :: ok | {error, Error},
  Error :: term().
%% ====================================================================
init(_Args) ->
  %% Start the Process Registry...
  application:start(nprocreg),

  %% Start Cowboy...
  application:start(cowboy),

  Port = 8000,
  DocRoot = "./site",
  StaticPaths = ["js/","images/","css/","nitrogen/","templates/"],

  Dispatch = init_dispatch(DocRoot, StaticPaths),

  %% Start the listener
  {ok, _} = cowboy:start_http(http, 100, [{port, Port}], [
    {env, [{dispatch, Dispatch}]},
    {max_keepalive, 50}
  ]),

  {ok, { {one_for_one, 5, 10}, []} }.



%% handle/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
  Request :: ping | get_version,
  Result :: ok | {ok, Response} | {error, Error} | pong | Version,
  Response :: term(),
  Version :: term(),
  Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  1;

handle(_ProtocolVersion, _Msg) ->
  ok.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0 <br />
%% Stops previously started applications (nprocreg and cowboy)
%% @end
-spec cleanup() -> Result when
  Result :: ok | {error, Error},
  Error :: timeout | term().
%% ====================================================================
cleanup() ->
  application:stop(nprocreg),
  application:stop(cowboy),
  ok.


%% ====================================================================
%% Auxiliary functions
%% ====================================================================

%% Compiles dispatch options to the format cowboy expects
init_dispatch(DocRoot,StaticPaths) ->
  Handler = cowboy_static,
  StaticDispatches = lists:map(fun(Dir) ->
    Path = reformat_path(Dir),
    Opts = [
      {mimetypes, {fun mimetypes:path_to_mimes/2, default}}
        | localized_dir_file(DocRoot, Dir)


    ],
    {Path,Handler,Opts}
  end, StaticPaths),

  %% HandlerModule will end up calling HandlerModule:handle(Req, HandlerOpts)
  HandlerModule = nitrogen_handler,
  HandlerOpts = [],

  %% Set up dispatch
  Dispatch = [
  %% Nitrogen will handle everything that's not handled in the StaticDispatches
    {'_', StaticDispatches ++ [{'_',HandlerModule , HandlerOpts}]}
  ],
  cowboy_router:compile(Dispatch).


localized_dir_file(DocRoot,Path) ->
  NewPath = case hd(Path) of
    $/ -> DocRoot ++ Path;
    _ -> DocRoot ++ "/" ++ Path
  end,
  _NewPath2 = case lists:last(Path) of
    $/ -> [{directory, NewPath}];
    _ ->
      Dir = filename:dirname(NewPath),
      File = filename:basename(NewPath),
      [
        {directory,Dir},
        {file,File}
      ]
  end.

%% Ensure the paths start with /, and if a path ends with /, then add "[...]" to it
reformat_path(Path) ->
  Path2 = case hd(Path) of
    $/ -> Path;
    $\ -> Path;
    _ -> [$/|Path]
  end,
  Path3 = case lists:last(Path) of
    $/ -> Path2 ++ "[...]";
    $\ -> Path2 ++ "[...]";
    _ -> Path2
  end,
  Path3.
