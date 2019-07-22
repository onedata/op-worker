%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, delete)
%%% corresponding to QoS management.
%%% @end
%%%-------------------------------------------------------------------
-module(op_qos).
-author("Michal Cwiertnia").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-export([op_logic_plugin/0]).
-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_qos.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, instance, private) -> true;

operation_supported(get, instance, private) -> true;
operation_supported(get, effective_qos, private) -> true;

operation_supported(delete, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{<<"expression">> => {binary, non_empty}},
    optional => #{<<"replicasNum">> => {integer, {not_lower_than, 1}}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = effective_qos}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:entity()} | op_logic:error().
fetch_entity(#op_req{operation = create, gri = #gri{aspect = instance}}) ->
    {ok, undefined};

fetch_entity(#op_req{operation = get, client = Client, gri = #gri{
    id = QosId,
    aspect = instance
}}) ->
    fetch_qos_entry(Client, QosId);

fetch_entity(#op_req{operation = get, gri = #gri{aspect = effective_qos}}) ->
    {ok, undefined};

fetch_entity(#op_req{operation = delete, client = Client, gri = #gri{
    id = QosId,
    aspect = instance
}}) ->
    fetch_qos_entry(Client, QosId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), op_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback authorize/2.
%%
%% Checks only membership in space.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), op_logic:entity()) -> boolean().
authorize(#op_req{client = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, client = Client, gri = #gri{
    id = FileGuid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = get, client = Client, gri = #gri{aspect = instance}},
    #qos_entry{file_guid = FileGuid}
) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = get, client = Client, gri = #gri{
    id = FileGuid,
    aspect = effective_qos
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = delete, client = Client, gri = #gri{aspect = instance}},
    #qos_entry{file_guid = FileGuid}
) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = instance}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = instance}}, #qos_entry{
    file_guid = Guid
}) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = Guid, aspect = effective_qos}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{aspect = instance}}, #qos_entry{
    file_guid = Guid
}) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = instance}} = Req) ->
    SessionId = Cl#client.session_id,
    QosExpression = maps:get(<<"expression">>, Req#op_req.data),
    ReplicasNum = maps:get(<<"replicasNum">>, Req#op_req.data, 1),

    case lfm:add_qos(SessionId, {guid, FileGuid}, QosExpression, ReplicasNum) of
        {ok, QosId} ->
            {ok, value, QosId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = effective_qos}}, _) ->
    SessionId = Cl#client.session_id,

    case lfm:get_file_qos(SessionId, {guid, FileGuid}) of
        {ok, {QosList, TargetStorages}} ->
            {ok, #{
                <<"qosList">> => QosList,
                <<"targetStorages">> => TargetStorages,
                <<"fulfilled">> => lfm_qos:check_qos_fulfilled(SessionId, QosList, {guid, FileGuid})
            }};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end;

get(#op_req{client = Cl, gri = #gri{id = QosId, aspect = instance}}, #qos_entry{
    expression = Expression,
    replicas_num = ReplicasNum
}) ->
    SessionId = Cl#client.session_id,
    {ok, #{
        <<"qosId">> => QosId,
        <<"expression">> => Expression,
        <<"replicasNum">> => ReplicasNum,
        <<"fulfilled">> => lfm_qos:check_qos_fulfilled(SessionId, QosId)
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{client = Cl, gri = #gri{id = QosId, aspect = instance}}) ->
    case lfm:remove_qos(Cl#client.session_id, QosId) of
        ok -> ok;
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_qos_entry(op_logic:client(), qos_entry:id()) ->
    {ok, #qos_entry{}} | ?ERROR_NOT_FOUND.
fetch_qos_entry(#client{session_id = SessionId}, QosId) ->
    case lfm:get_qos_details(SessionId, QosId) of
        {ok, QosEntry} ->
            {ok, QosEntry};
        _ ->
            ?ERROR_NOT_FOUND
    end.
