%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of tree_deletion_traverse.
%%% @end
%%%-------------------------------------------------------------------
-module(trash_test_SUITE).
-author("Jakub Kudzia").

-include("lfm_test_utils.hrl").
-include("rest_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
%%-include("modules/storage/traverse/storage_traverse.hrl").
%%-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").
%%-include_lib("ctool/include/test/assertions.hrl").
%%-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
%%-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    trash_dir_should_exist_test/1,
    move_to_trash_test/1,
    move_to_trash_and_delete_test/1]).


all() -> ?ALL([
    trash_dir_should_exist_test,
    move_to_trash_test,
    move_to_trash_and_delete_test
]).

-define(SPACE_ID, <<"space1">>).
-define(SPACE_GUID, ?SPACE_GUID(?SPACE_ID)).
-define(SPACE_GUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)).
-define(USER1, <<"user1">>).
-define(SESS_ID(Worker, Config), ?SESS_ID(?USER1, Worker, Config)).
-define(ATTEMPTS, 15).
-define(RAND_NAME(Prefix), <<Prefix/binary, (integer_to_binary(rand:uniform(1000)))/binary>>).
-define(RAND_DIR_NAME, ?RAND_NAME(<<"dir_">>)).
-define(RAND_FILE_NAME, ?RAND_NAME(<<"file_">>)).
-define(TRASH_DIR_GUID(SpaceId), fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId)).

-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, <<"user1">>, OtherHeaders)).

% todo testy z importem
% todo test, ze nie da sie usunac trasha gdzies powinien byc
% todo test, ze nie da sie zreplikowac trasha
% todo test, ze nie da sie zmovovać

%%%===================================================================
%%% Test functions
%%%===================================================================

trash_dir_should_exist_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),

    % TODO VFS-7064 uncomment after introducing links to trash directory
%%    % trash dir should be visible in the space on both providers
%%    ?assertMatch({ok, [{_, ?TRASH_DIR_NAME}]},
%%        lfm_proxy:get_children(W1, ?SESS_ID(W1, Config), {guid, ?SPACE_GUID}, 0, 10)),
%%    ?assertMatch({ok, [{_, ?TRASH_DIR_NAME}]},
%%        lfm_proxy:get_children(W2, ?SESS_ID(W2, Config), {guid, ?SPACE_GUID}, 0, 10)),

    % trash dir should be empty
    ?assertMatch({ok, #file_attr{name = ?TRASH_DIR_NAME}},
        lfm_proxy:stat(W1, ?SESS_ID(W1, Config), {guid, ?TRASH_DIR_GUID(?SPACE_ID)})),
    ?assertMatch({ok, #file_attr{name = ?TRASH_DIR_NAME}},
        lfm_proxy:stat(W2, ?SESS_ID(W2, Config), {guid, ?TRASH_DIR_GUID(?SPACE_ID)})),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, ?SESS_ID(W1, Config), {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10)),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, ?SESS_ID(W2, Config), {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10)).


move_to_trash_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    DirName = ?RAND_DIR_NAME,
    SessId1 = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    {ok, DirGuid} = lfm_proxy:mkdir(W1, ?SESS_ID(W1, Config), ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    DirCtx = file_ctx:new_by_guid(DirGuid),

    ok = move_to_trash(W1, DirCtx),

    lfm_test_utils:is_space_dir_empty(W1, ?SESS_ID(W1, Config), ?SPACE_ID, ?ATTEMPTS),
    lfm_test_utils:is_space_dir_empty(W2, ?SESS_ID(W2, Config), ?SPACE_ID, ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, _}]}, lfm_proxy:get_children(W1, SessId1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10)),
    ?assertMatch({ok, [{DirGuid, _}]}, lfm_proxy:get_children(W2, SessId2, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),


    StorageFileId = filename:join(["/", DirName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),

    % file registration should fail because there is a deletion marker added for the file
    % which prevents file to be imported
    ?assertMatch({ok, ?HTTP_400_BAD_REQUEST, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => DirName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => 10,
        <<"mode">> => <<"664">>,
        <<"autoDetectAttributes">> => false
    })).

move_to_trash_and_delete_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    DirName = ?RAND_DIR_NAME,
    FileName = ?RAND_FILE_NAME,
    SessId1 = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    {ok, DirGuid} = lfm_proxy:mkdir(W1, ?SESS_ID(W1, Config), ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    {ok, {FileGuid, Handle}} = lfm_proxy:create_and_open(W1, SessId1, DirGuid, FileName, ?DEFAULT_FILE_PERMS),
    lfm_proxy:close(W1, Handle),
    DirCtx = file_ctx:new_by_guid(DirGuid),

    ok = move_to_trash(W1, DirCtx),
    delete_from_trash(W1, DirCtx, SessId1),

    lfm_test_utils:is_space_dir_empty(W1, ?SESS_ID(W1, Config), ?SPACE_ID, ?ATTEMPTS),
    lfm_test_utils:is_space_dir_empty(W2, ?SESS_ID(W2, Config), ?SPACE_ID, ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, SessId2, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId1, {guid, FileGuid}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId1, {guid, DirGuid}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, FileGuid}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, DirGuid}), ?ATTEMPTS),


    StorageFileId = filename:join(["/", DirName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),

    % file registration should succeed because the file has already been deleted
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => DirName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => 10,
        <<"mode">> => <<"664">>,
        <<"autoDetectAttributes">> => false
    })).

%%%===================================================================
%%% Test bases
%%%===================================================================

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        NewConfig3 = sort_workers(NewConfig2),
        [W1 | _] = ?config(op_worker_nodes, NewConfig3),
        ok = rpc:call(W1, storage_import, set_manual_mode, [?SPACE_ID]),
        NewConfig3
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, ?MODULE]}
        | Config
    ].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    lfm_test_utils:clean_space(W1, ?SPACE_ID, ?ATTEMPTS),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

move_to_trash(Worker, FileCtx) ->
    ok = rpc:call(Worker, trash, move_to_trash, [FileCtx]).

delete_from_trash(Worker, FileCtx, SessId) ->
    UserCtx = rpc:call(Worker, user_ctx, new, [SessId]),
    rpc:call(Worker, trash, delete_from_trash, [FileCtx, UserCtx, false]).

register_file(Worker, Config, Body) ->
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),
    rest_test_utils:request(Worker, <<"data/register">>, post, Headers, json_utils:encode(Body)).
