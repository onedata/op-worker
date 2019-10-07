%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles gs rpc concerning transfer entities.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_gs_rpc).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    get_space_transfers/2,
    get_file_transfers/2
]).

-define(TRANSFER_GRI_ID(__TID), gri:serialize(#gri{
    type = op_transfer,
    id = __TID,
    aspect = instance,
    scope = private
})).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_space_transfers(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
get_space_transfers(?USER(UserId, _SessionId) = Auth, Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{
            <<"spaceId">> => {binary, non_empty},
            <<"state">> => {binary, [
                ?WAITING_TRANSFERS_STATE,
                ?ONGOING_TRANSFERS_STATE,
                ?ENDED_TRANSFERS_STATE
            ]},
            <<"limit">> => {integer, {not_lower_than, 1}}
        },
        optional => #{
            <<"index">> => {any, fun
                (null) ->
                    {true, undefined};
                (undefined) ->
                    true;
                (<<>>) ->
                    throw(?ERROR_BAD_VALUE_EMPTY(<<"index">>));
                (IndexBin) when is_binary(IndexBin) ->
                    true;
                (_) ->
                    false
            end},
            <<"offset">> => {integer, any}
        }
    }),
    SpaceId = maps:get(<<"spaceId">>, SanitizedData),
    State = maps:get(<<"state">>, SanitizedData),
    Limit = maps:get(<<"limit">>, SanitizedData),
    StartId = maps:get(<<"index">>, SanitizedData, undefined),
    Offset = maps:get(<<"offset">>, SanitizedData, 0),

    assert_space_membership_and_local_support(Auth, SpaceId),

    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS) of
        true ->
            {ok, TransferIds} = case State of
                ?WAITING_TRANSFERS_STATE ->
                    transfer:list_waiting_transfers(SpaceId, StartId, Offset, Limit);
                ?ONGOING_TRANSFERS_STATE ->
                    transfer:list_ongoing_transfers(SpaceId, StartId, Offset, Limit);
                ?ENDED_TRANSFERS_STATE ->
                    transfer:list_ended_transfers(SpaceId, StartId, Offset, Limit)
            end,

            {ok, [?TRANSFER_GRI_ID(Tid) || Tid <- TransferIds]};
        false ->
            ?ERROR_UNAUTHORIZED
    end.


-spec get_file_transfers(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
get_file_transfers(?USER(UserId, _SessionId) = Auth, Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{
            <<"guid">> => {binary, non_empty}
        },
        optional => #{
            <<"endedInfo">> => {binary, [<<"count">>, <<"ids">>]}
        }
    }),

    FileGuid = maps:get(<<"guid">>, SanitizedData),
    EndedInfo = maps:get(<<"endedInfo">>, SanitizedData, <<"count">>),

    SpaceId = file_id:guid_to_space_id(FileGuid),
    assert_space_membership_and_local_support(Auth, SpaceId),

    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS) of
        true ->
            {ok, #{
                ongoing := Ongoing,
                ended := Ended
            }} = transferred_file:get_transfers(FileGuid),

            {ok, #{
                <<"ongoing">> => [?TRANSFER_GRI_ID(Tid) || Tid <- Ongoing],
                <<"ended">> => case EndedInfo of
                    <<"count">> -> length(Ended);
                    <<"ids">> -> [?TRANSFER_GRI_ID(Tid) || Tid <- Ended]
                end
            }};
        false ->
            ?ERROR_UNAUTHORIZED
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_space_membership_and_local_support(aai:auth(), od_space:id()) ->
    ok | no_return().
assert_space_membership_and_local_support(Auth, SpaceId) ->
    case op_logic_utils:is_eff_space_member(Auth, SpaceId) of
        true ->
            op_logic_utils:assert_space_supported_locally(SpaceId);
        false ->
            throw(?ERROR_UNAUTHORIZED)
    end.
