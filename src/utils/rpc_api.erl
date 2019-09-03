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
-include_lib("ctool/include/api_errors.hrl").

-export([apply/2]).
-export([
    storage_new/4, storage_create/1, storage_safe_remove/1,
    storage_supports_any_space/1, storage_list_ids/0, storage_get_helpers/1,
    storage_get_luma_config_map/1, storage_select_helper/2,
    storage_update_admin_ctx/3, storage_update_helper_args/3,
    storage_set_insecure/3, storage_set_readonly/2, storage_set_luma_config/2,
    storage_update_luma_config/2, storage_update_name/2, get_storage_by_name/1,
    get_storage/1, storage_exists/1, storage_describe/1, get_space_storage/1,
    space_storage_get_storage_ids/1, space_storage_get_mounted_in_root/1,
    file_popularity_api_configure/2, file_popularity_api_get_configuration/1,
    invalidate_luma_cache/1, new_helper/5, new_luma_config/2,
    verify_storage_on_all_nodes/1, prepare_helper_args/2,
    prepare_user_ctx_params/2, autocleaning_configure/2,
    autocleaning_get_configuration/1, get_provider_id/0, get_access_token/0,
    is_connected_to_oz/0, is_registered/0, on_deregister/0,
    get_op_worker_version/0, provider_logic_update/1, support_space/2,
    space_storage_add/3, space_storage_delete/1, get_spaces/0,
    supports_space/1, get_space_details/1, get_provider_details/0,
    is_subdomain_delegated/0, set_delegated_subdomain/1, set_domain/1,
    space_quota_current_size/1, update_space_support_size/2,
    update_subdomain_delegation_ips/0, autocleaning_list_reports/4,
    autocleaning_get_run_report/1, autocleaning_status/1,
    autocleaning_force_start/1, force_oz_connection_start/0,
    provider_auth_save/2, get_root_token_file_path/0,
    get_storage_import_details/2, get_storage_update_details/2,
    modify_storage_import/3, modify_storage_update/3,
    storage_sync_monitoring_get_metric/3,
    storage_sync_monitoring_get_import_state/1,
    storage_sync_monitoring_get_update_state/1, restart_rtransfer_link/0,
    set_txt_record/3, remove_txt_record/1
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

-spec storage_new(storage:name(), [storage:helper()], boolean(),
    undefined | luma_config:config()) -> storage:doc().
storage_new(Name, Helpers, ReadOnly, LumaConfig) ->
    storage:new(Name, Helpers, ReadOnly, LumaConfig).


-spec storage_create(storage:doc()) -> ok | {error, term()}.
storage_create(StorageDoc) ->
    case storage:create(StorageDoc) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.


-spec storage_safe_remove(storage:id()) -> ok | {error, storage_in_use | term()}.
storage_safe_remove(StorageId) ->
    storage:safe_remove(StorageId).


-spec storage_supports_any_space(StorageId :: storage:id()) -> boolean().
storage_supports_any_space(StorageId) ->
    storage:supports_any_space(StorageId).


-spec storage_list_ids() -> {ok, [storage:id()]} | {error, term()}.
storage_list_ids() ->
    case storage:list() of
        {ok, Docs} -> {ok, lists:map(fun storage:get_id/1, Docs)};
        Error -> Error
    end.


-spec storage_get_helpers(storage:doc()) -> [helpers:helper()].
storage_get_helpers(StorageDoc) ->
    storage:get_helpers(StorageDoc).


-spec storage_get_luma_config_map(storage:doc()) -> map().
storage_get_luma_config_map(StorageDoc) ->
    storage:get_luma_config_map(StorageDoc).


-spec storage_select_helper(storage:id(), helper:name()) ->
    {ok, helpers:helper()} | {error, Reason :: term()}.
storage_select_helper(StorageId, HelperName) ->
    storage:select_helper(StorageId, HelperName).


-spec storage_update_admin_ctx(storage:id(), helper:name(), helper:user_ctx()) ->
    ok | {error, term()}.
storage_update_admin_ctx(StorageId, HelperName, Changes) ->
    storage:update_admin_ctx(StorageId, HelperName, Changes).


-spec storage_update_helper_args(storage:id(), helper:name(), helper:args()) ->
    ok | {error, term()}.
storage_update_helper_args(StorageId, HelperName, Changes) ->
    storage:update_helper_args(StorageId, HelperName, Changes).


-spec storage_set_insecure(storage:id(), helper:name(), Insecure :: boolean()) ->
    ok | {error, term()}.
storage_set_insecure(StorageId, HelperName, Insecure) ->
    storage:set_insecure(StorageId, HelperName, Insecure).


-spec storage_set_readonly(StorageId :: storage:id(), Readonly :: boolean()) ->
    ok | {error, term()}.
storage_set_readonly(StorageId, Readonly) ->
    storage:set_readonly(StorageId, Readonly).


-spec storage_set_luma_config(storage:id(), LumaConfig :: luma_config:config() | undefined) ->
    ok | {error, term()}.
storage_set_luma_config(StorageId, LumaConfig) ->
    storage:set_luma_config(StorageId, LumaConfig).


-spec storage_update_luma_config(storage:id(), Changes) -> ok | {error, term()}
    when Changes :: #{url => luma_config:url(), api_key => luma_config:api_key()}.
storage_update_luma_config(StorageId, Changes) ->
    storage:update_luma_config(StorageId, Changes).


-spec storage_update_name(StorageId :: storage:id(), NewName :: storage:name()) ->
    ok.
storage_update_name(StorageId, NewName) ->
    storage:update_name(StorageId, NewName).


-spec get_storage(storage:id()) -> {ok, storage:doc()} | {error, term()}.
get_storage(Key) ->
    storage:get(Key).


-spec get_storage_by_name(storage:name()) ->
    {ok, storage:doc()} | {error, term()}.
get_storage_by_name(Name) ->
    storage:select(Name).


-spec storage_exists(storage:id()) -> boolean().
storage_exists(StorageId) ->
    storage:exists(StorageId).


-spec storage_describe(storage:id()) ->
    {ok, #{binary() := binary() | boolean() | undefined}} | {error, term()}.
storage_describe(StorageId) ->
    storage:describe(StorageId).


-spec get_space_storage(space_storage:id()) ->
    {ok, space_storage:doc()} | {error, term()}.
get_space_storage(Key) ->
    space_storage:get(Key).


-spec space_storage_get_storage_ids(space_storage:id()) -> [storage:id()].
space_storage_get_storage_ids(SpaceId) ->
    space_storage:get_storage_ids(SpaceId).


-spec space_storage_get_mounted_in_root(space_storage:id()) -> [storage:id()].
space_storage_get_mounted_in_root(SpaceId) ->
    space_storage:get_mounted_in_root(SpaceId).


-spec file_popularity_api_configure(file_popularity_config:id(), map()) ->
    ok | {error, term()}.
file_popularity_api_configure(SpaceId, NewConfiguration) ->
    file_popularity_api:configure(SpaceId, NewConfiguration).


-spec file_popularity_api_get_configuration(file_popularity_config:id()) ->
    {ok, map()} | {error, term()}.
file_popularity_api_get_configuration(SpaceId) ->
    file_popularity_api:get_configuration(SpaceId).


-spec invalidate_luma_cache(storage:id()) -> ok.
invalidate_luma_cache(StorageId) ->
    luma_cache:invalidate(StorageId).


-spec new_helper(helper:name(), helper:args(), helper:user_ctx(), Insecure :: boolean(),
    helper:storage_path_type()) -> {ok, helpers:helper()}.
new_helper(HelperName, Args, AdminCtx, Insecure, StoragePathType) ->
    helper:new_helper(HelperName, Args, AdminCtx, Insecure, StoragePathType).


-spec new_luma_config(luma_config:url(), luma_config:api_key()) ->
    luma_config:config().
new_luma_config(URL, ApiKey) ->
    luma_config:new(URL, ApiKey).


-spec verify_storage_on_all_nodes(helpers:helper()) ->
    ok | {error, term()} | {error, term(), Stacktrace :: list()}.
verify_storage_on_all_nodes(Helper) ->
    storage_detector:verify_storage_on_all_nodes(Helper).


-spec prepare_helper_args(helper:name(), helper:args()) -> helper:args().
prepare_helper_args(HelperName, Params) ->
    helper_params:prepare_helper_args(HelperName, Params).


-spec prepare_user_ctx_params(helper:name(), helper:user_ctx()) -> helper:user_ctx().
prepare_user_ctx_params(HelperName, Params) ->
    helper_params:prepare_user_ctx_params(HelperName, Params).


-spec get_provider_id() -> {ok, od_provider:id()} | {error, term()}.
get_provider_id() ->
    provider_auth:get_provider_id().


-spec get_access_token() -> {ok, tokens:serialized()} | {error, term()}.
get_access_token() ->
    provider_auth:get_access_token().


-spec is_connected_to_oz() -> boolean().
is_connected_to_oz() ->
    oneprovider:is_connected_to_oz().


-spec is_registered() -> boolean().
is_registered() ->
    oneprovider:is_registered().


-spec on_deregister() -> ok.
on_deregister() ->
    oneprovider:on_deregister().


-spec get_op_worker_version() -> binary().
get_op_worker_version() ->
    oneprovider:get_version().


-spec provider_logic_update(Data :: #{binary() => term()}) ->
    ok | gs_protocol:error().
provider_logic_update(Data) ->
    provider_logic:update(Data).


-spec support_space(tokens:serialized(), SupportSize :: integer()) ->
    {ok, od_space:id()} | gs_protocol:error().
support_space(Token, SupportSize) ->
    provider_logic:support_space(Token, SupportSize).


-spec space_storage_add(od_space:id(), storage:id(), boolean()) ->
    {ok, od_space:id()} | {error, term()}.
space_storage_add(SpaceId, StorageId, MountInRoot) ->
    space_storage:add(SpaceId, StorageId, MountInRoot).


-spec space_storage_delete(space_storage:id()) -> ok | {error, term()}.
space_storage_delete(SpaceId) ->
    space_storage:delete(SpaceId).


-spec get_spaces() -> {ok, [od_space:id()]} | gs_protocol:error().
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
    {ok, #{atom() := term()}} | gs_protocol:error().
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
-spec get_provider_details() -> {ok, #{atom() := term()}} | gs_protocol:error().
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
    {true, Subdomain :: binary()} | false | gs_protocol:error().
is_subdomain_delegated() ->
    provider_logic:is_subdomain_delegated().


-spec set_delegated_subdomain(binary()) ->
    ok | {error, subdomain_exists} | gs_protocol:error().
set_delegated_subdomain(Subdomain) ->
    provider_logic:set_delegated_subdomain(Subdomain).


-spec set_domain(binary()) -> ok | gs_protocol:error().
set_domain(Domain) ->
    provider_logic:set_domain(Domain).


-spec space_quota_current_size(space_quota:id()) -> non_neg_integer().
space_quota_current_size(SpaceId) ->
    space_quota:current_size(SpaceId).


-spec update_space_support_size(od_space:id(), NewSupportSize :: integer()) ->
    ok | gs_protocol:error().
update_space_support_size(SpaceId, NewSupportSize) ->
    provider_logic:update_space_support_size(SpaceId, NewSupportSize).


-spec update_subdomain_delegation_ips() -> ok | error.
update_subdomain_delegation_ips() ->
    provider_logic:update_subdomain_delegation_ips().


-spec autocleaning_configure(od_space:id(), map()) -> ok | {error, term()}.
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
    autocleaning_api:status(SpaceId).


-spec autocleaning_force_start(od_space:id()) ->
    {ok, autocleaning:run_id()} | {error, term()}.
autocleaning_force_start(SpaceId) ->
    autocleaning_api:force_start(SpaceId).


-spec force_oz_connection_start() -> boolean().
force_oz_connection_start() ->
    oneprovider:force_oz_connection_start().


-spec provider_auth_save(od_provider:id(), tokens:serialized()) -> ok.
provider_auth_save(ProviderId, RootToken) ->
    provider_auth:save(ProviderId, RootToken).


-spec get_root_token_file_path() -> string().
get_root_token_file_path() ->
    provider_auth:get_root_token_file_path().


-spec get_storage_import_details(od_space:id(), storage:id()) ->
    space_strategy:config().
get_storage_import_details(SpaceId, StorageId) ->
    space_strategies:get_storage_import_details(SpaceId, StorageId).


-spec get_storage_update_details(od_space:id(), storage:id()) ->
    space_strategy:config().
get_storage_update_details(SpaceId, StorageId) ->
    space_strategies:get_storage_update_details(SpaceId, StorageId).


-spec modify_storage_import(od_space:id(), space_strategy:name(),
space_strategy:arguments()) ->
    {ok, datastore:key()} | {error, term()}.
modify_storage_import(SpaceId, StrategyName, Args) ->
    storage_sync:modify_storage_import(SpaceId, StrategyName, Args).


-spec modify_storage_update(od_space:id(), space_strategy:name(),
    space_strategy:arguments()) -> {ok, datastore:key()} | {error, term()}.
modify_storage_update(SpaceId, StrategyName, Args) ->
    storage_sync:modify_storage_update(SpaceId, StrategyName, Args).


-spec storage_sync_monitoring_get_metric(od_space:id(),
    storage_sync_monitoring:plot_counter_type(),
    storage_sync_monitoring:window()) -> proplists:proplist().
storage_sync_monitoring_get_metric(SpaceId, Type, Window) ->
    storage_sync_monitoring:get_metric(SpaceId, Type, Window).


-spec storage_sync_monitoring_get_import_state(od_space:id()) ->
    storage_import:state().
storage_sync_monitoring_get_import_state(SpaceId) ->
    storage_sync_monitoring:get_import_state(SpaceId).


-spec storage_sync_monitoring_get_update_state(od_space:id()) ->
    storage_update:state().
storage_sync_monitoring_get_update_state(SpaceId) ->
    storage_sync_monitoring:get_update_state(SpaceId).


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
