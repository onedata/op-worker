%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions related to the op_worker app.
%%% @end
%%%--------------------------------------------------------------------
-module(op_worker).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_env/1, get_env/2, set_env/2]).
-export([get_release_version/0, get_build_version/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_env(Key :: atom()) -> term() | no_return().
get_env(Key) ->
    case application:get_env(?APP_NAME, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            ?alert("Could not find required env variable for op-worker: ~p", [Key]),
            error({missing_env_variable, Key})
    end.


-spec get_env(Key :: atom(), Default) -> term() | Default.
get_env(Key, Default) ->
    application:get_env(?APP_NAME, Key, Default).


-spec set_env(Key :: atom(), Value :: term()) -> ok.
set_env(Key, Value) ->
    application:set_env(?APP_NAME, Key, Value).


-spec get_release_version() -> binary().
get_release_version() ->
    {_AppId, _AppName, OpVersion} = lists:keyfind(?APP_NAME, 1, application:loaded_applications()),
    list_to_binary(OpVersion).


-spec get_build_version() -> binary().
get_build_version() ->
    case op_worker:get_env(build_version, "unknown") of
        "" -> <<"unknown">>;
        Build -> list_to_binary(Build)
    end.