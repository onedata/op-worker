%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for read_event stream.
%%% @end
%%%--------------------------------------------------------------------
-module(read_event_handler).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("modules/events/types.hrl").
-include("proto/oneclient/fuse_messages.hrl").

-type req() :: cowboy_req:req().

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2,
    is_authorized/2, resource_exists/2, content_types_provided/2,
    content_types_accepted/2]).

%% resource functions
-export([get_file_distribution/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    Id = erlang:get(subscription_id),
    event:unsubscribe(Id).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:malformed_request/2
%%--------------------------------------------------------------------
-spec malformed_request(req(), #{}) -> {boolean(), req(), #{}}.
malformed_request(Req, State) ->
    rest_arg_parser:malformed_request(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), #{}) -> {boolean(), req(), #{}}.
resource_exists(Req, State) ->
    rest_existence_checker:resource_exists(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) -> {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_file_distribution}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) ->
    {[{atom() | binary(), atom()}], req(), #{}}.
content_types_accepted(Req, State) ->
    {[
        {'*', replicate_file}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Handles GET with "application/json" content-type
%%--------------------------------------------------------------------
-spec get_file_distribution(req(), #{}) -> {term(), req(), #{}}.
get_file_distribution(Req, #{attributes := #file_attr{uuid = Guid}, auth := _Auth} = State) -> %todo add autorization
    Self = self(),
    Id = make_ref(),
    {Timeout, Req2} = get_timeout(Req),
    ReadSub = event_subscriptions:read_subscription(
        fun
            ([], _Ctx) ->
                ok;
            (Events, Ctx) ->
                FilteredEvents = lists:filter( %todo use admission rule
                    fun(#event{object = #read_event{file_uuid = EventGuid}}) ->
                        EventGuid =:= Guid
                    end, Events),
                Self ! {Id, FilteredEvents, Ctx}
        end
    ),
    {ok, ReadSubId} = event:subscribe(ReadSub),
    erlang:put(subscription_id, ReadSubId),

    StreamFun = fun(SendChunk) ->
        event_loop(SendChunk, Id, Timeout, Req2, State)
    end,
    Req3 = cowboy_req:set_resp_body_fun(chunked, StreamFun, Req2),
    cowboy_req:reply(?HTTP_OK, [], Req3).

%%--------------------------------------------------------------------
%% @doc
%% Listens for events and pushes them to the socket
%% @end
%%--------------------------------------------------------------------
-spec event_loop(function(), reference(), timeout(), req(), #{}) -> {ok, req()} | no_return().
event_loop(SendChunk, Id, Timeout, Req, State) ->
    receive
        {Id, [_ | _] = Events, _Ctx} ->
            lists:map(
                fun(#event{counter = Counter, object = #read_event{size = Size, blocks = Blocks}}) ->
                    ParsedBlocks = [[O,S] || #file_block{offset = O, size = S} <- Blocks],
                    JsonEvent = json_utils:encode([
                        {<<"type">>, <<"read_event">>},
                        {<<"count">>, Counter},
                        {<<"size">>, Size},
                        {<<"blocks">>, ParsedBlocks}]),
                    SendChunk(<<JsonEvent/binary, "\r\n">>)
                end, Events),
            event_loop(SendChunk, Id, Timeout, Req, State)
    after
        Timeout ->
            {ok, Req}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get timeout from request's query string
%% @end
%%--------------------------------------------------------------------
-spec get_timeout(req()) -> {timeout(), req()}.
get_timeout(Req) ->
    {RawTimeout, Req2} = cowboy_req:qs_val(<<"timeout">>, Req, infinity),
    case RawTimeout of
        infinity ->
            {infinity, Req2};
        Number ->
            {binary_to_integer(Number), Req2}
    end.