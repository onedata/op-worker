%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_space records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_space records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(space_logic).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/errors.hrl").

-export([get/2, get_protected_data/2]).
-export([force_fetch/1]).
-export([get_name/2]).
-export([get_eff_users/2, has_eff_user/2, has_eff_user/3]).
-export([has_eff_privilege/3, has_eff_privileges/3, get_eff_privileges/2]).
-export([assert_has_eff_privilege/3]).
-export([is_owner/2]).
-export([get_eff_groups/2, get_shares/2, get_local_storages/1,
    get_local_supporting_storage/1, get_provider_storages/2, get_storages_by_provider/1,
    get_all_storage_ids/1, get_support_size/2, get_support_parameters/2]).
-export([get_provider_ids/1, get_provider_ids/2]).
-export([update_support_parameters/2]).
-export([is_supported/2, is_supported/3]).
-export([is_supported_by_storage/2]).
-export([has_readonly_support_from/2]).
-export([can_view_user_through_space/3, can_view_user_through_space/4]).
-export([can_view_group_through_space/3, can_view_group_through_space/4]).
-export([harvest_metadata/5]).
-export([get_harvesters/1]).
-export([group_spaces_by_name/2, extend_space_name/2]).
-export([on_space_supported/1]).

-define(HARVEST_METADATA_TIMEOUT, application:get_env(
    ?APP_NAME, graph_sync_harvest_metadata_request_timeout, 120000
)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves space doc by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:doc()} | errors:error().
get(?GUEST_SESS_ID, SpaceId) ->
    % Guest session is a virtual session fully managed by provider, and it needs
    % access to space info to serve public data such as shares.
    get(?ROOT_SESS_ID, SpaceId);
get(SessionId, SpaceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves space doc restricted to protected data by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:doc()} | errors:error().
get_protected_data(SessionId, SpaceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance, scope = protected},
        subscribe = true
    }).


-spec get_name(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:name()} | errors:error().
get_name(SessionId, SpaceId) ->
    case get_protected_data(SessionId, SpaceId) of
        {ok, #document{value = #od_space{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


-spec force_fetch(od_space:id()) -> {ok, od_space:doc()} | errors:error().
force_fetch(SpaceId) ->
    gs_client_worker:force_fetch_entity(#gri{type = od_space, id = SpaceId, aspect = instance}).


-spec get_eff_users(gs_client_worker:client(), od_space:id()) ->
    {ok, #{od_user:id() => [privileges:space_privilege()]}} | errors:error().
get_eff_users(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{eff_users = EffUsers}}} ->
            {ok, EffUsers};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_user(od_space:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_space{eff_users = EffUsers}}, UserId) ->
    maps:is_key(UserId, EffUsers).


-spec has_eff_user(gs_client_worker:client(), od_space:id(), od_user:id()) ->
    boolean().
has_eff_user(SessionId, SpaceId, UserId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            has_eff_user(SpaceDoc, UserId);
        _ ->
            false
    end.


-spec has_eff_privilege(od_space:doc() | od_space:id(), od_user:id(),
    privileges:space_privilege()) -> boolean().
has_eff_privilege(SpaceDocOrId, UserId, Privilege) ->
    has_eff_privileges(SpaceDocOrId, UserId, [Privilege]).


-spec has_eff_privileges(od_space:doc() | od_space:id(), od_user:id(),
    [privileges:space_privilege()]) -> boolean().
has_eff_privileges(#document{value = #od_space{eff_users = EffUsers}} = SpaceDoc, UserId, Privileges) ->
    UserPrivileges = maps:get(UserId, EffUsers, []),
    % space owners have all the privileges, regardless of those assigned
    lists_utils:is_subset(Privileges, UserPrivileges) orelse is_owner(SpaceDoc, UserId);
has_eff_privileges(SpaceId, UserId, Privileges) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{} = SpaceDoc} ->
            has_eff_privileges(SpaceDoc, UserId, Privileges);
        _ ->
            false
    end.


-spec get_eff_privileges(od_space:doc() | od_space:id(), od_user:id()) ->
    {ok, [privileges:space_privilege()]} | errors:error().
get_eff_privileges(#document{value = #od_space{eff_users = EffUsers}} = SpaceDoc, UserId) ->
    case is_owner(SpaceDoc, UserId) of
        true -> {ok, privileges:space_privileges()};
        false -> {ok, maps:get(UserId, EffUsers, [])}
    end;
get_eff_privileges(SpaceId, UserId) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{} = SpaceDoc} ->
            get_eff_privileges(SpaceDoc, UserId);
        {error, _} = Error ->
            Error
    end.


-spec assert_has_eff_privilege(od_space:id(), od_user:id(), privileges:space_privilege()) -> ok.
assert_has_eff_privilege(SpaceId, UserId, Privilege) ->
    % call by module to mock in tests
    case space_logic:has_eff_privilege(SpaceId, UserId, Privilege) of
        true ->
            ok;
        false ->
            throw({error, ?EPERM})
    end.


-spec is_owner(od_space:id() | od_space:doc(), od_user:id()) -> boolean().
is_owner(SpaceId, UserId) when is_binary(SpaceId) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{} = SpaceDoc} ->
            is_owner(SpaceDoc, UserId);
        _ ->
            false
    end;
is_owner(#document{value = #od_space{owners = Owners}}, UserId) ->
    lists:member(UserId, Owners).


-spec get_eff_groups(gs_client_worker:client(), od_space:id()) ->
    {ok, #{od_group:id() => [privileges:space_privilege()]}} | errors:error().
get_eff_groups(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{eff_groups = EffGroups}}} ->
            {ok, EffGroups};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_group(od_space:doc(), od_group:id()) -> boolean().
has_eff_group(#document{value = #od_space{eff_groups = EffGroups}}, GroupId) ->
    maps:is_key(GroupId, EffGroups).


-spec get_shares(gs_client_worker:client(), od_space:id()) ->
    {ok, [od_share:id()]} | errors:error().
get_shares(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{shares = Shares}}} ->
            {ok, Shares};
        {error, _} = Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% @TODO VFS-5497 Remove after allowing to support one space with many storages on one provider
%% This function returns StorageId for given SpaceId.
%% @end
%%-------------------------------------------------------------------
-spec get_local_supporting_storage(od_space:id()) -> {ok, storage:id()} | errors:error().
get_local_supporting_storage(SpaceId) ->
    % called by module to be mocked in tests
    case space_logic:get_local_storages(SpaceId) of
        {ok, []} -> {error, space_not_supported};
        {ok, [StorageId | _]} -> {ok, StorageId};
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage ids supporting given space, belonging to this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_local_storages(od_space:id()) -> {ok, [storage:id()]} | errors:error().
get_local_storages(SpaceId) ->
    case get_provider_storages(SpaceId, oneprovider:get_id()) of
        {ok, ProviderStorages} -> {ok, maps:keys(ProviderStorages)};
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns map in the form #{storage:id() => storage:access_type()}
%% with storages supporting given space, belonging to ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_storages(od_space:id(), od_provider:id()) ->
    {ok, #{storage:id() => storage:access_type()}} | errors:error().
get_provider_storages(SpaceId, ProviderId) when is_binary(SpaceId) ->
    case get_storages_by_provider(SpaceId) of
        {ok, #{ProviderId := ProviderStorages}} ->
            {ok, ProviderStorages};
        {ok, _} ->
            ?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, ProviderId);
        {error, _} = Error ->
            Error
    end.


-spec get_storages_by_provider(od_space:id()) ->
    {ok, #{od_provider:id() => #{storage:id() => storage:access_type()}}} | errors:error().
get_storages_by_provider(SpaceId) when is_binary(SpaceId) ->
    case space_logic:get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{value = #od_space{storages_by_provider = StoragesByProvider}}} ->
            {ok, StoragesByProvider};
        {error, _} = Error ->
            Error
    end.


-spec get_all_storage_ids(od_space:id()) -> {ok, [storage:id()]} | errors:error().
get_all_storage_ids(SpaceId) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{value = #od_space{storages = AllStorages}}} ->
            {ok, maps:keys(AllStorages)};
        {error, _} = Error ->
            Error
    end.


-spec get_support_size(od_space:id(), od_provider:id()) -> {ok, non_neg_integer()} | errors:error().
get_support_size(SpaceId, ProviderId) ->
    % call via module to mock in tests
    case space_logic:get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{value = #od_space{providers = ProviderSupports}}} ->
            case maps:get(ProviderId, ProviderSupports, undefined) of
                undefined ->
                    ?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, ProviderId);
                SupportSize ->
                    {ok, SupportSize}
            end;
        {error, _} = Error ->
            Error
    end.


-spec get_support_parameters(od_space:id(), od_provider:id()) ->
    {ok, support_parameters:record()} | errors:error().
get_support_parameters(SpaceId, ProviderId) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{value = #od_space{support_parameters_registry = SupportParametersRegistry}}} ->
            {ok, support_parameters_registry:get_entry(ProviderId, SupportParametersRegistry)};
        {error, _} = Error ->
            Error
    end.


-spec get_provider_ids(od_space:id()) ->
    {ok, [od_provider:id()]} | errors:error().
get_provider_ids(SpaceId) ->
    get_provider_ids(?ROOT_SESS_ID, SpaceId).


-spec get_provider_ids(gs_client_worker:client(), od_space:id()) ->
    {ok, [od_provider:id()]} | errors:error().
get_provider_ids(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{providers = Providers}}} ->
            {ok, maps:keys(Providers)};
        {error, _} = Error ->
            Error
    end.


-spec update_support_parameters(od_space:id(), support_parameters:record()) ->
    ok | errors:error().
update_support_parameters(SpaceId, SupportParametersOverlay) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{
            type = od_space,
            id = SpaceId,
            aspect = {support_parameters, oneprovider:get_id()},
            scope = private
        },
        data = jsonable_record:to_json(SupportParametersOverlay, support_parameters)
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        space_logic:force_fetch(SpaceId)
    end).


-spec is_supported(od_space:doc() | od_space:record(), od_provider:id()) ->
    boolean().
is_supported(#od_space{providers = Providers}, ProviderId) ->
    maps:is_key(ProviderId, Providers);
is_supported(#document{value = Space}, ProviderId) ->
    is_supported(Space, ProviderId).


-spec is_supported(gs_client_worker:client(), od_space:id(), od_provider:id()) ->
    boolean().
is_supported(SessionId, SpaceId, ProviderId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            is_supported(SpaceDoc, ProviderId);
        ?ERROR_NOT_FOUND ->  % the space has been deleted or never existed
            false;
        ?ERROR_FORBIDDEN ->  % forbidden access due to lack of support
            false
    end.


-spec is_supported_by_storage(od_space:id(), storage:id()) -> boolean().
is_supported_by_storage(SpaceId, StorageId) ->
    case get_all_storage_ids(SpaceId) of
        {ok, AllStorageIds} -> lists:member(StorageId, AllStorageIds);
        _ -> false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether ALL storages with which the ProviderId supports
%% the space are readonly.
%% @end
%%--------------------------------------------------------------------
-spec has_readonly_support_from(od_space:id(), od_provider:id()) -> boolean().
has_readonly_support_from(SpaceId, ProviderId) ->
    case get_provider_storages(SpaceId, ProviderId) of
        {ok, ProviderStorages} when map_size(ProviderStorages) =:= 0 ->
            % if the map is empty, this provider does not support the space and has
            % no knowledge about other supports to determine if they are readonly
            false;
        {ok, ProviderStorages} ->
            lists:all(fun(AccessMode) ->
                AccessMode =:= ?READONLY
            end, maps:values(ProviderStorages));
        {error, _} = Error ->
            throw(Error)
    end.


-spec can_view_user_through_space(gs_client_worker:client(), od_space:id(),
    ClientUserId :: od_user:id(), TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_space(SessionId, SpaceId, ClientUserId, TargetUserId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            can_view_user_through_space(SpaceDoc, ClientUserId, TargetUserId);
        _ ->
            false
    end.


-spec can_view_user_through_space(od_space:doc(), ClientUserId :: od_user:id(),
    TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_space(SpaceDoc, ClientUserId, TargetUserId) ->
    has_eff_privilege(SpaceDoc, ClientUserId, ?SPACE_VIEW) andalso
        has_eff_user(SpaceDoc, TargetUserId).


-spec can_view_group_through_space(gs_client_worker:client(), od_space:id(),
    ClientUserId :: od_user:id(), od_group:id()) -> boolean().
can_view_group_through_space(SessionId, SpaceId, ClientUserId, GroupId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            can_view_group_through_space(SpaceDoc, ClientUserId, GroupId);
        _ ->
            false
    end.


-spec can_view_group_through_space(od_space:doc(), ClientUserId :: od_user:id(),
    od_group:id()) -> boolean().
can_view_group_through_space(SpaceDoc, ClientUserId, GroupId) ->
    has_eff_privilege(SpaceDoc, ClientUserId, ?SPACE_VIEW) andalso
        has_eff_group(SpaceDoc, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Pushes batch of metadata changes to Onezone.
%% Metadata are harvested for given SpaceId and Destination.
%% Destination is the structure that describes target Harvesters and
%% associated Indices.
%% MaxStreamSeq and MaxSeq are sent to allow for tracking progress of
%% harvesting. MaxStreamSeq is the highest sequence number processed by the
%% calling harvesting_stream. MaxSeq is the highest sequence number in given
%% space.
%% This function can return following values:
%%     * {ok, FailureMap :: #{od_harvester:id() =>
%%           #{od_harvester:index() => FirstFailedSeq :: couchbase_changes:seq()}
%%        }}
%%        FirstFailedSeq is the first sequence number on which harvesting
%%        failed for given index.
%%        If harvesting succeeds for whole Destination, the map is empty.
%%     * {error, _} :: errors:error()
%%
%% NOTE!!!
%% If you introduce any changes in this function, please ensure that
%% docs in {@link harvesting_stream} module are up to date.
%% @end
%%--------------------------------------------------------------------
-spec harvest_metadata(od_space:id(), harvesting_destination:destination(),
    harvesting_batch:batch_entries(), couchbase_changes:seq(),
    couchbase_changes:seq()) -> {ok, harvesting_result:failure_map()} | errors:error().
harvest_metadata(SpaceId, Destination, Batch, MaxStreamSeq, MaxSeq) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = SpaceId,
            aspect = harvest_metadata, scope = private
        },
        data = #{
            <<"destination">> => Destination,
            <<"maxSeq">> => MaxSeq,
            <<"maxStreamSeq">> => MaxStreamSeq,
            <<"batch">> => Batch
        }
    }, ?HARVEST_METADATA_TIMEOUT).


-spec get_harvesters(od_space:doc() | od_space:id()) ->
    {ok, [od_harvester:id()]} | errors:error().
get_harvesters(#document{value = #od_space{harvesters = Harvesters}}) ->
    {ok, Harvesters};
get_harvesters(SpaceId) ->
    case space_logic:get(?ROOT_SESS_ID, SpaceId) of
        {ok, Doc} ->
            get_harvesters(Doc);
        {error, _} = Error ->
            ?error("space_logic:get_harvesters(~p) failed due to ~p", [SpaceId, Error]),
            Error
    end.


-spec on_space_supported(od_space:id()) -> ok.
on_space_supported(SpaceId) ->
    ok = qos_logic:reevaluate_all_impossible_qos_in_space(SpaceId).


-spec group_spaces_by_name(gs_client_worker:client(), [od_space:id()]) -> #{od_space:name() => [od_space:id()]}.
group_spaces_by_name(SessId, Spaces) ->
    lists:foldl(fun(SpaceId, Acc) ->
        case get_name(SessId, SpaceId) of
            {ok, SpaceName} ->
                Acc#{SpaceName => [SpaceId | maps:get(SpaceName, Acc, [])]};
            ?ERROR_NOT_FOUND ->
                Acc;
            ?ERROR_FORBIDDEN ->
                Acc
        end
    end, #{}, Spaces).


-spec extend_space_name(od_space:name(), od_space:id()) -> file_meta:name().
extend_space_name(SpaceName, SpaceId) ->
    <<SpaceName/binary, (?SPACE_NAME_ID_SEPARATOR)/binary, SpaceId/binary>>.
