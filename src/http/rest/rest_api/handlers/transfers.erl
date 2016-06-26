%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for listing and managing transfers.
%%% @end
%%%--------------------------------------------------------------------
-module(transfers).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").


%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, delete_resource/2]).

%% resource functions
-export([list_transfers/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, State) ->
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) -> {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, list_transfers}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}'
%% @doc Cancels a scheduled or active transfer. Returns 400 in case the transfer
%% is already completed, canceled or failed.\n
%%
%% HTTP method: DELETE
%%
%% @param tid Transfer ID.
%%--------------------------------------------------------------------
-spec delete_resource(req(), #{}) -> {term(), req(), #{}}.
delete_resource(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),

    #{id := Id} = State2,

    ok = transfer:stop(Id),
    {true, Req2, State2}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers'
%% @doc Returns the list of all transfer IDs.
%%
%% HTTP method: GET
%%
%% @param status Allows to limit the returned transfers only to transfers with specific status.\n
%% @param limit Allows to limit the number of returned transfers only to the last N transfers.\n
%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}'
%% @doc Returns status of specific transfer. In case the transfer has been
%% scheduled for entire folder, the result is a list of transfer statuses for
%% each item in the folder.
%%
%% HTTP method: GET
%%
%% @param tid Transfer ID.
%%--------------------------------------------------------------------
-spec list_transfers(req(), #{}) -> {term(), req(), #{}}.
list_transfers(Req, State = #{list_all := true}) ->
    {State2, Req2} = validator:parse_limit(Req, State),
    {State3, Req3} = validator:parse_status(Req2, State2),

    #{auth := Auth, status := Status, limit := Limit} = State3,

    Transfers = session:get_transfers(Auth),
    LimitedTransfers = lists:sublist(Transfers, Limit),
    FilteredTransfers =
        case Status of
            undefined ->
                LimitedTransfers;
            _ ->
                lists:filter(fun(Pid) ->
                    {ok, TransferStatus} = transfer:get_status(Pid),
                    atom_to_binary(TransferStatus, utf8) =:= Status
                end, LimitedTransfers)
        end,
    Response = json_utils:encode(FilteredTransfers),
    {Response, Req3, State3};
list_transfers(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),

    #{id := Id} = State2,

    {ok, Transfer} = transfer:get(Id),
    Response = json_utils:encode(Transfer),
    {Response, Req2, State2}.