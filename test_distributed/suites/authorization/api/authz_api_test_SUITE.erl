%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of authorization mechanism
%%% with corresponding operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("proto/oneclient/fuse_messages.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    mkdir/1,
    get_children/1,
    get_children_attrs/1,
    get_child_attr/1,
    mv_dir/1,
    rm_dir/1,

    create_file/1,
    open_for_read/1,
    open_for_write/1,
    open_for_rdwr/1,
    create_and_open/1,
    truncate/1,
    mv_file/1,
    rm_file/1,

    get_parent/1,
    get_file_path/1,
    %% TODO
%%    resolve_guid/1,
    stat/1,

    set_perms/1,
    check_read_perms/1,
    check_write_perms/1,
    check_rdwr_perms/1,

    create_share/1,
    remove_share/1,
    share_perms_are_checked_only_up_to_share_root/1,

    get_acl/1,
    set_acl/1,
    remove_acl/1,

    get_transfer_encoding/1,
    set_transfer_encoding/1,
    get_cdmi_completion_status/1,
    set_cdmi_completion_status/1,
    get_mimetype/1,
    set_mimetype/1,

    get_custom_metadata/1,
    set_custom_metadata/1,
    remove_custom_metadata/1,
    get_xattr/1,
    list_xattr/1,
    set_xattr/1,
    remove_xattr/1,
    get_file_distribution/1,
    get_historical_dir_size_stats/1,
    get_file_storage_locations/1,

    add_qos_entry/1,
    get_qos_entry/1,
    remove_qos_entry/1,
    get_effective_file_qos/1,
    check_qos_status/1
]).

all() -> [
    mkdir,
    get_children,
    get_children_attrs,
    get_child_attr,
    mv_dir,
    rm_dir,

    create_file,
    open_for_read,
    open_for_write,
    open_for_rdwr,
    create_and_open,
    truncate,
    mv_file,
    rm_file,

    get_parent,
    get_file_path,
    resolve_guid,
    stat,

    set_perms,
    check_read_perms,
    check_write_perms,
    check_rdwr_perms,

    create_share,
    remove_share,
    share_perms_are_checked_only_up_to_share_root,

    get_acl,
    set_acl,
    remove_acl,

    get_transfer_encoding,
    set_transfer_encoding,
    get_cdmi_completion_status,
    set_cdmi_completion_status,
    get_mimetype,
    set_mimetype,

    get_custom_metadata,
    set_custom_metadata,
    remove_custom_metadata,
    get_xattr,
    list_xattr,
    set_xattr,
    remove_xattr,
    get_file_distribution,
    get_historical_dir_size_stats,
    get_file_storage_locations,

    add_qos_entry,
    get_qos_entry,
    remove_qos_entry,
    get_effective_file_qos,
    check_qos_status
].


-define(RUN_AUTHZ_DIR_API_TEST(__CONFIG),
    authz_dir_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_REG_FILE_API_TEST(__CONFIG),
    authz_reg_file_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_FILE_COMMON_API_TEST(__CONFIG),
    authz_file_common_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_PERMS_API_TEST(__CONFIG),
    authz_perms_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_SHARE_API_TEST(__CONFIG),
    authz_share_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_ACL_API_TEST(__CONFIG),
    authz_acl_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_CDMI_API_TEST(__CONFIG),
    authz_cdmi_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_FILE_METADATA_API_TEST(__CONFIG),
    authz_file_metadata_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_QOS_API_TEST(__CONFIG),
    authz_qos_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).

-define(ATTEMPTS, 10).


%%%===================================================================
%%% Test functions
%%%===================================================================


mkdir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


get_children(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


get_children_attrs(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


get_child_attr(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


mv_dir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


rm_dir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


create_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


open_for_read(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


open_for_write(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


open_for_rdwr(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


create_and_open(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


truncate(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


mv_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


rm_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


get_parent(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


get_file_path(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


resolve_guid(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


stat(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


set_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


check_read_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


check_write_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


check_rdwr_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


create_share(Config) ->
    ?RUN_AUTHZ_SHARE_API_TEST(Config).


remove_share(Config) ->
    ?RUN_AUTHZ_SHARE_API_TEST(Config).


share_perms_are_checked_only_up_to_share_root(Config) ->
    ?RUN_AUTHZ_SHARE_API_TEST(Config).


get_acl(Config) ->
    ?RUN_AUTHZ_ACL_API_TEST(Config).


set_acl(Config) ->
    ?RUN_AUTHZ_ACL_API_TEST(Config).


remove_acl(Config) ->
    ?RUN_AUTHZ_ACL_API_TEST(Config).


get_transfer_encoding(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


set_transfer_encoding(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


get_cdmi_completion_status(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


set_cdmi_completion_status(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


get_mimetype(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


set_mimetype(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


get_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


set_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


remove_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


list_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


set_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


remove_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_file_distribution(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_historical_dir_size_stats(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_file_storage_locations(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


add_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


get_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


remove_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


get_effective_file_qos(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


check_qos_status(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
%%    StorageType = ?RAND_ELEMENT([posix, s3]),
    StorageType = posix,  % TODO

    ModulesToLoad = [?MODULE, authz_api_test_runner],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = get_onenv_scenario(StorageType),
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            delete_spaces_from_previous_run(),
            [{storage_type, StorageType}, {storage_id, find_storage_id(StorageType)} | NewConfig]
        end
    }).


%% @private
-spec get_onenv_scenario(posix | s3) -> list().
get_onenv_scenario(posix) -> "1op_posix";
get_onenv_scenario(s3) -> "1op_s3".


%% @private
-spec delete_spaces_from_previous_run() -> ok.
delete_spaces_from_previous_run() ->
    AllTestCases = all(),

    RemovedSpaces = lists:filter(fun(SpaceId) ->
        SpaceDetails = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceDetails),

        Exists = lists:member(binary_to_atom(SpaceName), AllTestCases),
        Exists andalso ozw_test_rpc:delete_space(SpaceId),

        Exists
    end, ozw_test_rpc:list_spaces()),

    ?assertEqual([], lists_utils:intersect(opw_test_rpc:get_spaces(krakow), RemovedSpaces), ?ATTEMPTS),

    ok.


%% @private
-spec find_storage_id(posix | s3) -> storage:id().
find_storage_id(StorageType) ->
    StorageTypeBin = atom_to_binary(StorageType),

    [StorageId] = lists:filter(fun(StorageId) ->
        StorageDetails = opw_test_rpc:storage_describe(krakow, StorageId),
        StorageTypeBin == maps:get(<<"type">>, StorageDetails)
    end, opw_test_rpc:get_storages(krakow)),

    StorageId.


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = space_owner,
        users = [user1, user2],
        supports = [#support_spec{
            provider = krakow,
            size = 10000000,
            storage_spec = ?config(storage_id, Config)
        }]
    }),
    [{space_id, SpaceId} | lfm_proxy:init(Config)].


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
