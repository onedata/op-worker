%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of token data access caveats.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_data_access_caveats_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("storage_files_test_SUITE.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    test1/1
]).

groups() -> [
    {listing_tests, [parallel], [
        test1
    ]}
].

all() -> [
    {group, listing_tests}
].


%%%===================================================================
%%% Listing tests
%%%===================================================================


-define(LS_SPACE, space_krk_par_p).
-define(LS_USER, user2).

-define(LS_FILE_TREE_SPEC, [
    #dir_spec{
        name = <<"listing_dir1">>,
        shares = [#share_spec{}],
        children = [#file_spec{name = <<"listing_file", ($0 + Num)>>} || Num <- lists:seq(1, 5)]
    },
    #dir_spec{
        name = <<"listing_dir2">>
    },
    #dir_spec{
        name = <<"listing_dir3">>,
        children = [#file_spec{name = <<"listing_file", ($0 + Num)>>} || Num <- lists:seq(1, 5)]
    }
]).

-define(LS_PATH(__ABBREV), ls_build_path(__ABBREV)).
-define(LS_GUID(__ABBREV), ls_get_guid(?LS_PATH(__ABBREV))).
-define(LS_OBJECT_ID(__ABBREV), ls_get_object_id(?LS_PATH(__ABBREV))).
-define(LS_ENTRY(__ABBREV), ls_get_entry(?LS_PATH(__ABBREV))).


test1(_Config) ->
    ?assertEqual(
        {ok, [?LS_ENTRY("d1;f1"), ?LS_ENTRY("d1;f2"), ?LS_ENTRY("d1;f3"), ?LS_ENTRY("d1;f4"), ?LS_ENTRY("d1;f5")]},
        ls_with_caveats(?LS_GUID("d1"), #cv_data_path{whitelist = [?LS_PATH("d1")]})
    ).


%% @private
ls_setup() ->
    SpacePath = filepath_utils:join([<<"/">>, oct_background:get_space_id(?LS_SPACE)]),

    UserId = oct_background:get_user_id(?LS_USER),
    MainToken = create_oz_temp_access_token(UserId),
    node_cache:put(ls_tests_main_token, MainToken),

    FileTreeObjects = onenv_file_test_utils:create_and_sync_file_tree(
        user1, ?LS_SPACE, ?LS_FILE_TREE_SPEC
    ),
    node_cache:put(ls_tests_root_guids, [Object#object.guid || Object <- utils:ensure_list(FileTreeObjects)]),

    FileTreeDesc = ls_describe_file_tree(#{}, SpacePath, FileTreeObjects),
    node_cache:put(ls_tests_file_tree, FileTreeDesc).


%% @private
ls_teardown() ->
    lists:foreach(fun(Guid) ->
        onenv_file_test_utils:rm_and_sync_file(user1, Guid)
    end, node_cache:get(ls_tests_root_guids)).


% TODO mv to onenv_ct
%% @private
create_oz_temp_access_token(UserId) ->
    OzNode = ?RAND_ELEMENT(oct_background:get_zone_nodes()),

    Auth = ?USER(UserId),
    Now = ozw_test_rpc:timestamp_seconds(OzNode),
    Token = ozw_test_rpc:create_user_temporary_token(OzNode, Auth, UserId, #{
        <<"type">> => ?ACCESS_TOKEN,
        <<"caveats">> => [#cv_time{valid_until = Now + 360000}]
    }),

    {ok, SerializedToken} = tokens:serialize(Token),
    SerializedToken.


%% @private
ls_describe_file_tree(Desc, ParentPath, FileTreeObjects) when is_list(FileTreeObjects) ->
    lists:foldl(fun(FileObject, DescAcc) ->
        ls_describe_file_tree(DescAcc, ParentPath, FileObject)
    end, Desc, FileTreeObjects);

ls_describe_file_tree(Desc, ParentPath, #object{
    guid = Guid,
    name = Name,
    children = Children
}) ->
    Path = filepath_utils:join([ParentPath, Name]),
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),

    NewDesc = Desc#{Path => #{
        name => Name,
        guid => Guid,
        object_id => ObjectId
    }},

    lists:foldl(fun(Child, DescAcc) ->
        ls_describe_file_tree(DescAcc, Path, Child)
    end, NewDesc, utils:ensure_defined(Children, [])).


%% @private
ls_build_path(Abbrev) ->
    ls_build_path(
        string:split(str_utils:to_binary(Abbrev), <<";">>, all),
        filepath_utils:join([<<"/">>, oct_background:get_space_id(?LS_SPACE)])
    ).


%% @private
ls_build_path([], Path) ->
    Path;
ls_build_path([<<"f", FileSuffix/binary>> | LeftoverTokens], Path) ->
    NewPath = filepath_utils:join([Path, <<"listing_file", FileSuffix/binary>>]),
    ls_build_path(LeftoverTokens, NewPath);
ls_build_path([<<"d", DirSuffix/binary>> | LeftoverTokens], Path) ->
    NewPath = filepath_utils:join([Path, <<"listing_dir", DirSuffix/binary>>]),
    ls_build_path(LeftoverTokens, NewPath).


%% @private
ls_get_name(Path) ->
    kv_utils:get([Path, name], node_cache:get(ls_tests_file_tree)).


%% @private
ls_get_guid(Path) ->
    kv_utils:get([Path, guid], node_cache:get(ls_tests_file_tree)).


%% @private
ls_get_object_id(Path) ->
    kv_utils:get([Path, object_id], node_cache:get(ls_tests_file_tree)).


%% @private
ls_get_entry(Path) ->
    {ls_get_guid(Path), ls_get_name(Path)}.


%% @private
ls_with_caveats(Guid, Caveats) ->
    Node = oct_background:get_random_provider_node(?RAND_ELEMENT([krakow, paris])),
    UserId = oct_background:get_user_id(?LS_USER),

    MainToken = node_cache:get(ls_tests_main_token),
    LsToken = tokens:confine(MainToken, Caveats),
    LsSessId = permissions_test_utils:create_session(Node, UserId, LsToken),

    lfm_proxy:get_children(Node, LsSessId, ?FILE_REF(Guid), 0, 100).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(listing_tests, Config) ->
    ls_setup(),
    Config.


end_per_group(listing_tests, Config) ->
    ls_teardown(),
    Config.


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
