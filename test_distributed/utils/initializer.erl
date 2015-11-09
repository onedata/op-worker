%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Urility functions for initializing things like session or storage configuration.
%%% @end
%%%--------------------------------------------------------------------
-module(initializer).
-author("Tomasz Lichon").

-include_lib("ctool/include/test/test_utils.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([setup_session/3, teardown_sesion/2, setup_storage/1, teardown_storage/1]).

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% API
%%%===================================================================j

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec setup_session(Worker :: node(), [{UserNum :: non_neg_integer(), [SpaceIds :: binary()]}], Config :: term()) -> NewConfig :: term().
setup_session(_Worker, [], Config) ->
    Config;
setup_session(Worker, [{UserNum, SpaceIds} | R], Config) ->
    Self = self(),

    Name = fun(Text, Num) -> name(Text, Num) end,

    SessId = Name("session_id", UserNum),
    UserId = Name("user_id", UserNum),
    Iden = #identity{user_id = UserId},
    UserName = Name("username", UserNum),

    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self])),
    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    {ok, _} = rpc:call(Worker, onedata_user, create, [
        #document{key = UserId, value = #onedata_user{
            name = UserName, space_ids = SpaceIds
        }}
    ]),
    ?assertEqual({ok, onedata_user_setup}, test_utils:receive_msg(
        onedata_user_setup, ?TIMEOUT)),
    [
        {{spaces, UserNum}, SpaceIds}, {{user_id, UserNum}, UserId}, {{session_id, UserNum}, SessId},
        {{fslogic_ctx, UserNum}, #fslogic_ctx{session = Session}}
        | setup_session(Worker, R, Config)
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes existing test session.
%% @end
%%--------------------------------------------------------------------
-spec teardown_sesion(Worker :: node(), Config :: term()) -> NewConfig :: term().
teardown_sesion(Worker, Config) ->
    lists:foldl(fun
        ({{session_id, _}, SessId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),
            Acc;
        ({{spaces, _}, SpaceIds}, Acc) ->
            lists:foreach(fun(SpaceId) ->
                ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [SpaceId]))
            end, SpaceIds),
            Acc;
        ({{user_id, _}, UserId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, onedata_user, delete, [UserId])),
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [UserId])),
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_path:spaces_uuid(UserId)])),
            Acc;
        ({{fslogic_ctx, _}, _}, Acc) ->
            Acc;
        (Elem, Acc) ->
            [Elem | Acc]
    end, [], Config).

%%--------------------------------------------------------------------
%% @doc
%% Creates test storage
%% @end
%%--------------------------------------------------------------------
-spec setup_storage(Config :: list()) -> list().
setup_storage(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    TmpDir = generator:gen_storage_dir(Config),
    %% @todo: use shared storage
    "" = rpc:call(Worker, os, cmd, ["mkdir -p " ++ TmpDir]),
    {ok, StorageId} = rpc:call(Worker, storage, create, [#document{value = fslogic_storage:new_storage(<<"Test">>,
        [fslogic_storage:new_helper_init(<<"DirectIO">>, #{<<"root_path">> => list_to_binary(TmpDir)})])}]),
    [{storage_id, StorageId}, {storage_dir, TmpDir} | Config].

%%--------------------------------------------------------------------
%% @doc
%% Reomves test storage
%% @end
%%--------------------------------------------------------------------
-spec teardown_storage(Config :: list()) -> string().
teardown_storage(Config) ->
    TmpDir = ?config(storage_dir, Config),
    [Worker | _] = ?config(op_worker_nodes, Config),
    "" = rpc:call(Worker, os, cmd, ["rm -rf " ++ TmpDir]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

name(Text, Num) ->
    list_to_binary(Text ++ "_" ++ integer_to_list(Num)).
