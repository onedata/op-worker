%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: DBSync changes hooks management for dbsync worker
%% @end
%% ===================================================================
-module(dbsync_hooks).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("files_common.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([register/1, unregister/1]).


%% register/1
%% ====================================================================
%% @doc Registers given callback as document changes hook.
%%      Note that the callback will be called for each document change that is not
%%      guaranteed to be in order.
%% @end
-spec register(
    Fun :: fun((DbName :: string() | binary(),
                    SpaceId :: binary(),
                    DocUUID :: string() | binary(),
                    Document :: #db_document{}) -> any())) ->
    HookId :: binary().
%% ====================================================================
register(Fun) when is_function(Fun) ->
    HookId = gen_hook_id(),
    dbsync_state:call(fun(_State) ->
        Hooks =
            case dbsync_state:get(hooks) of
                undefined -> [];
                Hooks1 when is_list(Hooks1) ->
                    Hooks1
            end,
        dbsync_state:set(hooks, [{HookId, Fun} | Hooks])
    end),
    HookId.


%% unregister/1
%% ====================================================================
%% @doc Removes given by UUID hook that was registered earlier.
%% @end
-spec unregister(HookId :: binary()) -> ok.
%% ====================================================================
unregister(HookId) ->
    dbsync_state:call(fun(_State) ->
        Hooks =
            case dbsync_state:get(hooks) of
                undefined -> [];
                Hooks0 when is_list(Hooks0) ->
                    Hooks0
            end,
        Hooks1 = [Hook || {HookId0, _} = Hook <- Hooks, HookId0 =/= HookId],
        dbsync_state:set(hooks, Hooks1)
    end),
    ok.


%% gen_hook_id/0
%% ====================================================================
%% @doc Returns new UUID.
%% @end
-spec gen_hook_id() -> UUID :: binary().
%% ====================================================================
gen_hook_id() ->
    utils:ensure_binary(dao_helper:gen_uuid()).