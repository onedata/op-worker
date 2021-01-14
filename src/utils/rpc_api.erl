%%%-------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module grouping all functions rpc called by Onepanel
%%% to simplify detecting their usage.
%%% @end
%%%-------------------------------------------------------------------
-module(rpc_api).
-author("Wojciech Geisler").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

-export([apply/2]).
-export([
    storage_create/6,
    storage_safe_remove/1,
    storage_supports_any_space/1,
    storage_list_ids/0,
    storage_get_helper/1,
    storage_update_admin_ctx/2,
    storage_update_helper_args/2,
    storage_update_readonly_and_imported/3,
    storage_set_qos_parameters/2,
    storage_update_luma_config/2,
    storage_update_name/2,
    storage_exists/1,
    storage_describe/1,
    storage_is_imported_storage/1,
    storage_get_luma_feed/1,
    storage_verify_configuration/3,
    luma_clear_db/1,
    luma_storage_users_get_and_describe/2,
    luma_storage_users_store/3,
    luma_storage_users_update/3,
    luma_storage_users_delete/2,
    luma_spaces_display_defaults_get_and_describe/2,
    luma_spaces_display_defaults_delete/2,
    luma_spaces_display_defaults_store/3,
    luma_spaces_posix_storage_defaults_get_and_describe/2,
    luma_spaces_posix_storage_defaults_store/3,
    luma_spaces_posix_storage_defaults_delete/2,
    luma_onedata_users_get_by_uid_and_describe/2,
    luma_onedata_users_store_by_uid/3,
    luma_onedata_users_delete_uid_mapping/2,
    luma_onedata_users_get_by_acl_user_and_describe/2,
    luma_onedata_users_store_by_acl_user/3,
    luma_onedata_users_delete_acl_user_mapping/2,
    luma_onedata_groups_get_and_describe/2,
    luma_onedata_groups_store/3,
    luma_onedata_groups_delete/2,
    new_helper/3,
    new_luma_config/1,
    new_luma_config_with_external_feed/2,
    verify_storage_on_all_nodes/2,
    prepare_helper_args/2,
    prepare_user_ctx_params/2,
    get_helper_args/1,
    get_helper_admin_ctx/1,
    space_logic_get_storage_ids/1,
    file_popularity_api_configure/2,
    file_popularity_api_get_configuration/1,
    autocleaning_configure/2,
    autocleaning_get_configuration/1,
    autocleaning_list_reports/4,
    autocleaning_get_run_report/1,
    autocleaning_status/1,
    autocleaning_force_run/1,
    autocleaning_cancel_run/1,
    get_provider_id/0,
    get_access_token/0,
    get_identity_token/0,
    is_connected_to_oz/0,
    is_registered/0,
    on_deregister/0,
    get_op_worker_version/0,
    provider_logic_update/1,
    support_space/3,
    revoke_space_support/1,
    get_spaces/0,
    supports_space/1,
    get_space_details/1,
    get_provider_details/0,
    is_subdomain_delegated/0,
    set_delegated_subdomain/1,
    set_domain/1,
    space_quota_current_size/1,
    update_space_support_size/2,
    update_subdomain_delegation_ips/0,
    force_oz_connection_start/0,
    provider_auth_save/2,
    get_root_token_file_path/0,
    storage_import_get_configuration/1,
    storage_import_set_manual_import/1,
    storage_import_configure_auto_import/2,
    storage_import_start_scan/1,
    storage_import_stop_scan/1,
    storage_import_get_stats/3,
    storage_import_get_info/1,
    storage_import_get_manual_example/1,
    restart_rtransfer_link/0,
    set_txt_record/3,
    remove_txt_record/1
]).


%%%===================================================================
%%% API entrypoint
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Wraps function invocation to wrap 'throw' exceptions in badrpc tuple
%% as if they were 'error' exceptions.
%% @end
%%--------------------------------------------------------------------
-spec apply(Function :: atom(), Args :: [term()]) ->
    Result :: term() | {badrpc, {'EXIT', {Reason, Stacktrace}}} when
    Reason :: term(), Stacktrace :: list().
apply(Function, Args) ->
    try
        erlang:apply(?MODULE, Function, Args)
    catch
        throw:Error ->
            Stacktrace = erlang:get_stacktrace(),
            {badrpc, {'EXIT', {Error, Stacktrace}}}
    end.


%%%===================================================================
%%% Exposed functions
%%%===================================================================

-spec storage_create(storage:name(), helpers:helper(),
    storage:luma_config(), storage:imported(), storage:readonly(), storage:qos_parameters()) ->
    storage:id() | {error, term()}.
storage_create(Name, Helpers, LumaConfig, ImportedStorage, Readonly, QosParameters) ->
    storage:create(Name, Helpers, LumaConfig, ImportedStorage, Readonly, QosParameters).


-spec storage_safe_remove(storage:id()) -> ok | {error, storage_in_use | term()}.
storage_safe_remove(StorageId) ->
    storage:delete(StorageId).


-spec storage_supports_any_space(storage:id()) -> boolean().
storage_supports_any_space(StorageId) ->
    storage:supports_any_space(StorageId).


-spec storage_list_ids() -> {ok, [storage:id()]} | {error, term()}.
storage_list_ids() ->
    provider_logic:get_storage_ids().


-spec storage_get_helper(storage:id()) -> {ok, helpers:helper()}.
storage_get_helper(StorageId) ->
    {ok, storage:get_helper(StorageId)}.


-spec storage_update_admin_ctx(storage:id(), helper:user_ctx()) ->
    ok | {error, term()}.
storage_update_admin_ctx(StorageId, Changes) ->
    storage:update_helper_admin_ctx(StorageId, Changes).


-spec storage_update_helper_args(storage:id(), helper:args()) ->
    ok | {error, term()}.
storage_update_helper_args(StorageId, Changes) ->
    storage:update_helper_args(StorageId, Changes).


-spec storage_update_readonly_and_imported(storage:id(), boolean(), boolean()) ->
    ok | {error, term()}.
storage_update_readonly_and_imported(StorageId, Readonly, Imported) ->
    storage:update_readonly_and_imported(StorageId, Readonly, Imported).


-spec storage_set_qos_parameters(storage:id(), storage:qos_parameters()) ->
    ok | errors:error().
storage_set_qos_parameters(StorageId, QosParameters) ->
    storage:set_qos_parameters(StorageId, QosParameters).


-spec storage_update_luma_config(storage:id(),
    Changes :: luma_config:config() | luma_config:diff()) -> ok | {error, term()}.
storage_update_luma_config(StorageId, Changes) ->
    storage:update_luma_config(StorageId, Changes).


-spec storage_update_name(storage:id(), NewName :: storage:name()) -> ok.
storage_update_name(StorageId, NewName) ->
    storage:update_name(StorageId, NewName).


-spec storage_exists(storage:id()) -> boolean().
storage_exists(StorageId) ->
    storage:exists(StorageId).


-spec storage_describe(storage:id()) ->
    {ok, #{binary() := binary() | boolean() | undefined}} | {error, term()}.
storage_describe(StorageId) ->
    storage:describe(StorageId).


-spec storage_is_imported_storage(storage:id()) -> boolean().
storage_is_imported_storage(StorageId) ->
    storage:is_imported(StorageId).

-spec storage_get_luma_feed(storage:id() | storage:data()) -> luma:feed().
storage_get_luma_feed(Storage) ->
    storage:get_luma_feed(Storage).


-spec storage_verify_configuration(storage:id() | storage:name(), storage:config(), helpers:helper()) ->
    ok | {error, term()}.
storage_verify_configuration(IdOrName, Configuration, Helper) ->
    storage:verify_configuration(IdOrName, Configuration, Helper).


-spec luma_clear_db(storage:id()) -> ok.
luma_clear_db(StorageId) ->
    luma:clear_db(StorageId).


-spec luma_storage_users_get_and_describe(storage:id(), od_user:id()) ->
    {ok, luma_storage_user:user_map()} | {error, term()}.
luma_storage_users_get_and_describe(Storage, UserId) ->
    luma_storage_users:get_and_describe(Storage, UserId).


-spec luma_storage_users_store(storage:id(), od_user:id() | luma_onedata_user:user_map(),
    luma_storage_user:user_map()) ->
    {ok, od_user:id()} | {error, term()}.
luma_storage_users_store(Storage, OnedataUserMap, StorageUserMap) ->
    luma_storage_users:store(Storage, OnedataUserMap, StorageUserMap).


-spec luma_storage_users_update(storage:id(), od_user:id(), luma_storage_user:user_map()) ->
    ok | {error, term()}.
luma_storage_users_update(Storage, UserId, StorageUserMap) ->
    luma_storage_users:update(Storage, UserId, StorageUserMap).


-spec luma_storage_users_delete(storage:id(), od_user:id()) -> ok.
luma_storage_users_delete(Storage, UserId) ->
    luma_storage_users:delete(Storage, UserId).


-spec luma_spaces_display_defaults_get_and_describe(storage:id(), od_space:id()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
luma_spaces_display_defaults_get_and_describe(Storage, SpaceId) ->
    luma_spaces_display_defaults:get_and_describe(Storage, SpaceId).


-spec luma_spaces_display_defaults_store(storage:id() | storage:data(), od_space:id(),
    luma_posix_credentials:credentials_map()) -> ok | {error, term()}.
luma_spaces_display_defaults_store(Storage, SpaceId, DisplayDefaults) ->
    luma_spaces_display_defaults:store(Storage, SpaceId, DisplayDefaults).


-spec luma_spaces_display_defaults_delete(storage:id(), od_space:id()) -> ok.
luma_spaces_display_defaults_delete(Storage, SpaceId) ->
    luma_spaces_display_defaults:delete(Storage, SpaceId).


-spec luma_spaces_posix_storage_defaults_get_and_describe(storage:id(), od_space:id()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
luma_spaces_posix_storage_defaults_get_and_describe(Storage, SpaceId) ->
    luma_spaces_posix_storage_defaults:get_and_describe(Storage, SpaceId).


-spec luma_spaces_posix_storage_defaults_store(storage:id() | storage:data(), od_space:id(),
    luma_posix_credentials:credentials_map()) -> ok | {error, term()}.
luma_spaces_posix_storage_defaults_store(Storage, SpaceId, PosixDefaults) ->
    luma_spaces_posix_storage_defaults:store(Storage, SpaceId, PosixDefaults).


-spec luma_spaces_posix_storage_defaults_delete(storage:id(), od_space:id()) -> ok.
luma_spaces_posix_storage_defaults_delete(Storage, SpaceId) ->
    luma_spaces_posix_storage_defaults:delete(Storage, SpaceId).


-spec luma_onedata_users_get_by_uid_and_describe(storage:id() | storage:data(), luma:uid()) ->
    {ok, luma_onedata_user:user_map()} | {error, term()}.
luma_onedata_users_get_by_uid_and_describe(Storage, Uid) ->
    luma_onedata_users:get_by_uid_and_describe(Storage, Uid).


-spec luma_onedata_users_store_by_uid(storage:id() | storage:data(), luma:uid(),
    luma_onedata_user:user_map()) -> ok | {error, term()}.
luma_onedata_users_store_by_uid(Storage, Uid, OnedataUser) ->
    luma_onedata_users:store_by_uid(Storage, Uid, OnedataUser).


-spec luma_onedata_users_delete_uid_mapping(storage:id(), luma:uid()) -> ok | {error, term()}.
luma_onedata_users_delete_uid_mapping(Storage, Uid) ->
    luma_onedata_users:delete_uid_mapping(Storage, Uid).

-spec luma_onedata_users_get_by_acl_user_and_describe(storage:id() | storage:data(), luma:acl_who()) ->
    {ok, luma_onedata_user:user_map()} | {error, term()}.
luma_onedata_users_get_by_acl_user_and_describe(Storage, AclUser) ->
    luma_onedata_users:get_by_acl_user_and_describe(Storage, AclUser).


-spec luma_onedata_users_store_by_acl_user(storage:id() | storage:data(), luma:acl_who(),
    luma_onedata_user:user_map()) -> ok | {error, term()}.
luma_onedata_users_store_by_acl_user(Storage, AclUser, OnedataUser) ->
    luma_onedata_users:store_by_acl_user(Storage, AclUser, OnedataUser).


-spec luma_onedata_users_delete_acl_user_mapping(storage:id(), luma:acl_who()) -> ok | {error, term()}.
luma_onedata_users_delete_acl_user_mapping(Storage, AclUser) ->
    luma_onedata_users:delete_acl_user_mapping(Storage, AclUser).


-spec luma_onedata_groups_get_and_describe(storage:id() | storage:data(), luma:acl_who()) ->
    {ok, luma_onedata_group:group_map()} | {error, term()}.
luma_onedata_groups_get_and_describe(Storage, AclGroup) ->
    luma_onedata_groups:get_and_describe(Storage, AclGroup).


-spec luma_onedata_groups_store(storage:id() | storage:data(), luma:acl_who(),
    luma_onedata_group:group_map()) -> ok | {error, term()}.
luma_onedata_groups_store(Storage, AclGroup, OnedataGroup) ->
    luma_onedata_groups:store(Storage, AclGroup, OnedataGroup).


-spec luma_onedata_groups_delete(storage:id(), luma:acl_who()) -> ok | {error, term()}.
luma_onedata_groups_delete(Storage, AclGroup) ->
    luma_onedata_groups:delete(Storage, AclGroup).


-spec new_helper(helper:name(), helper:args(), helper:user_ctx()) -> {ok, helpers:helper()}.
new_helper(HelperName, Args, AdminCtx) ->
    helper:new_helper(HelperName, Args, AdminCtx).


-spec new_luma_config(luma_config:feed()) -> luma_config:config().
new_luma_config(Mode) ->
    luma_config:new(Mode).


-spec new_luma_config_with_external_feed(luma_config:url(), luma_config:api_key()) ->
    luma_config:config().
new_luma_config_with_external_feed(URL, ApiKey) ->
    luma_config:new_with_external_feed(URL, ApiKey).


-spec verify_storage_on_all_nodes(helpers:helper(), luma_config:feed()) -> ok | errors:error().
verify_storage_on_all_nodes(Helper, LumaMode) ->
    storage_detector:verify_storage_on_all_nodes(Helper, LumaMode).


-spec prepare_helper_args(helper:name(), helper:args()) -> helper:args().
prepare_helper_args(HelperName, Params) ->
    helper_params:prepare_helper_args(HelperName, Params).


-spec prepare_user_ctx_params(helper:name(), helper:user_ctx()) -> helper:user_ctx().
prepare_user_ctx_params(HelperName, Params) ->
    helper_params:prepare_user_ctx_params(HelperName, Params).


-spec get_helper_args(helpers:helper()) -> helper:args().
get_helper_args(Helper) ->
    helper:get_args(Helper).


-spec get_helper_admin_ctx(helpers:helper()) -> helper:user_ctx().
get_helper_admin_ctx(Helper) ->
    helper:get_admin_ctx(Helper).


-spec space_logic_get_storage_ids(od_space:id()) -> {ok, [storage:id()]}.
space_logic_get_storage_ids(SpaceId) ->
    space_logic:get_local_storage_ids(SpaceId).


-spec file_popularity_api_configure(file_popularity_config:id(), map()) ->
    ok | errors:error() | {error, term()}.
file_popularity_api_configure(SpaceId, NewConfiguration) ->
    file_popularity_api:configure(SpaceId, NewConfiguration).


-spec file_popularity_api_get_configuration(file_popularity_config:id()) ->
    {ok, map()} | {error, term()}.
file_popularity_api_get_configuration(SpaceId) ->
    file_popularity_api:get_configuration(SpaceId).


-spec get_provider_id() -> {ok, od_provider:id()} | {error, term()}.
get_provider_id() ->
    provider_auth:get_provider_id().


-spec get_access_token() -> {ok, tokens:serialized()} | {error, term()}.
get_access_token() ->
    provider_auth:acquire_access_token().


-spec get_identity_token() -> {ok, tokens:serialized()} | {error, term()}.
get_identity_token() ->
    provider_auth:acquire_identity_token().


-spec is_connected_to_oz() -> boolean().
is_connected_to_oz() ->
    gs_channel_service:is_connected().


-spec is_registered() -> boolean().
is_registered() ->
    oneprovider:is_registered().


-spec on_deregister() -> ok.
on_deregister() ->
    gs_hooks:handle_deregistered_from_oz().


-spec get_op_worker_version() -> binary().
get_op_worker_version() ->
    oneprovider:get_version().


-spec provider_logic_update(Data :: #{binary() => term()}) ->
    ok | errors:error().
provider_logic_update(Data) ->
    provider_logic:update(Data).


-spec support_space(storage:id(), tokens:serialized(), SupportSize :: integer()) ->
    {ok, od_space:id()} | errors:error().
support_space(StorageId, Token, SupportSize) ->
    storage:support_space(StorageId, Token, SupportSize).


-spec revoke_space_support(od_space:id()) -> ok | {error, term()}.
revoke_space_support(SpaceId) ->
    {ok, StorageIds} = space_logic:get_local_storage_ids(SpaceId),
    StorageId = hd(StorageIds),
    storage:revoke_space_support(StorageId, SpaceId).

-spec get_spaces() -> {ok, [od_space:id()]} | errors:error().
get_spaces() ->
    provider_logic:get_spaces().


-spec supports_space(od_space:id()) -> boolean().
supports_space(SpaceId) ->
    provider_logic:supports_space(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves details of given space using current provider's auth
%% and translates them to a map.
%% @end
%%--------------------------------------------------------------------
-spec get_space_details(od_space:id()) ->
    {ok, #{atom() := term()}} | errors:error().
get_space_details(SpaceId) ->
    case space_logic:get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{value = Record}} ->
            {ok, #{
                name => Record#od_space.name,
                direct_users => Record#od_space.direct_users,
                eff_users => Record#od_space.eff_users,
                direct_groups => Record#od_space.direct_groups,
                eff_groups => Record#od_space.eff_groups,
                providers => Record#od_space.providers,
                shares => Record#od_space.shares,
                harvesters => Record#od_space.harvesters
            }};
        {error, Error} -> {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns current provider's data in a map.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_details() -> {ok, #{atom() := term()}} | errors:error().
get_provider_details() ->
    case provider_logic:get() of
        {ok, #document{key = Id, value = Record}} ->
            {ok, #{
                id => Id,
                name => Record#od_provider.name,
                admin_email => Record#od_provider.admin_email,
                subdomain_delegation => Record#od_provider.subdomain_delegation,
                domain => Record#od_provider.domain,
                subdomain => Record#od_provider.subdomain,
                longitude => Record#od_provider.longitude,
                latitude => Record#od_provider.latitude
            }};
        Error -> Error
    end.


-spec is_subdomain_delegated() ->
    {true, Subdomain :: binary()} | false | errors:error().
is_subdomain_delegated() ->
    provider_logic:is_subdomain_delegated().


-spec set_delegated_subdomain(binary()) ->
    ok | errors:error().
set_delegated_subdomain(Subdomain) ->
    provider_logic:set_delegated_subdomain(Subdomain).


-spec set_domain(binary()) -> ok | errors:error().
set_domain(Domain) ->
    provider_logic:set_domain(Domain).


-spec space_quota_current_size(space_quota:id()) -> non_neg_integer().
space_quota_current_size(SpaceId) ->
    space_quota:current_size(SpaceId).


-spec update_space_support_size(od_space:id(), NewSupportSize :: integer()) ->
    ok | errors:error().
update_space_support_size(SpaceId, NewSupportSize) ->
    {ok, StorageIds} = space_logic:get_local_storage_ids(SpaceId),
    StorageId = hd(StorageIds),
    storage:update_space_support_size(StorageId, SpaceId, NewSupportSize).


-spec update_subdomain_delegation_ips() -> ok | error.
update_subdomain_delegation_ips() ->
    provider_logic:update_subdomain_delegation_ips().


-spec autocleaning_configure(od_space:id(), map()) ->
    ok | errors:error() | {error, term()}.
autocleaning_configure(SpaceId, Configuration) ->
    autocleaning_api:configure(SpaceId, Configuration).


-spec autocleaning_get_configuration(od_space:id()) -> map().
autocleaning_get_configuration(SpaceId) ->
    autocleaning_api:get_configuration(SpaceId).


-spec autocleaning_list_reports(od_space:id(), autocleaning:run_id() | undefined,
    autocleaning_run_links:offset(), autocleaning_run_links:list_limit()) ->
    {ok, [autocleaning:run_id()]}.
autocleaning_list_reports(SpaceId, Index, Offset, Limit) ->
    autocleaning_api:list_reports(SpaceId, Index, Offset, Limit).


-spec autocleaning_get_run_report(autocleaning_run:id()) ->
    {ok, map()} | {error, term()}.
autocleaning_get_run_report(RunId) ->
    autocleaning_api:get_run_report(RunId).


-spec autocleaning_status(od_space:id()) -> map().
autocleaning_status(SpaceId) ->
    autocleaning_api:get_status(SpaceId).


-spec autocleaning_force_run(od_space:id()) ->
    {ok, autocleaning:run_id()} | {error, term()}.
autocleaning_force_run(SpaceId) ->
    autocleaning_api:force_run(SpaceId).


-spec autocleaning_cancel_run(od_space:id()) -> ok.
autocleaning_cancel_run(SpaceId) ->
    autocleaning_api:cancel_run(SpaceId).


-spec force_oz_connection_start() -> boolean().
force_oz_connection_start() ->
    gs_channel_service:force_start_connection().


-spec provider_auth_save(od_provider:id(), tokens:serialized()) -> ok.
provider_auth_save(ProviderId, RootToken) ->
    provider_auth:save(ProviderId, RootToken).


-spec get_root_token_file_path() -> string().
get_root_token_file_path() ->
    provider_auth:get_root_token_file_path().


-spec storage_import_get_configuration(od_space:id()) ->
    {ok, storage_import:scan_config_map()} | {error, term()}.
storage_import_get_configuration(SpaceId) ->
    storage_import:get_configuration(SpaceId).


-spec storage_import_set_manual_import(od_space:id()) -> ok | {error, term()}.
storage_import_set_manual_import(SpaceId) ->
    storage_import:set_manual_mode(SpaceId).


-spec storage_import_configure_auto_import(od_space:id(), storage_import:scan_config_map()) ->
    ok | {error, term()}.
storage_import_configure_auto_import(SpaceId, ScanConfig) ->
    storage_import:set_or_configure_auto_mode(SpaceId, ScanConfig).


-spec storage_import_start_scan(od_space:id()) -> ok.
storage_import_start_scan(SpaceId) ->
    storage_import:start_auto_scan(SpaceId).


-spec storage_import_stop_scan(od_space:id()) -> ok.
storage_import_stop_scan(SpaceId) ->
    storage_import:stop_auto_scan(SpaceId).


-spec storage_import_get_stats(od_space:id(),
    [storage_import_monitoring:plot_counter_type()],
    storage_import_monitoring:window()) -> {ok, storage_import:stats()}.
storage_import_get_stats(SpaceId, Type, Window) ->
    storage_import:get_stats(SpaceId, Type, Window).


-spec storage_import_get_info(od_space:id()) -> {ok, json_utils:json_term()} | {error, term()}.
storage_import_get_info(SpaceId) ->
    storage_import:get_info(SpaceId).


-spec storage_import_get_manual_example(od_space:id()) ->
    {ok, binary()}.
storage_import_get_manual_example(SpaceId) ->
    storage_import:get_manual_example(SpaceId).


-spec restart_rtransfer_link() -> ok | {error, not_running}.
restart_rtransfer_link() ->
    rtransfer_config:restart_link().


-spec set_txt_record(Name :: binary(), Content :: binary(),
    TTL :: non_neg_integer() | undefined) -> ok | no_return().
set_txt_record(Name, Content, TTL) ->
    provider_logic:set_txt_record(Name, Content, TTL).


-spec remove_txt_record(Name :: binary()) -> ok | no_return().
remove_txt_record(Name) ->
    provider_logic:remove_txt_record(Name).
