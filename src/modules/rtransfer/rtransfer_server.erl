%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% {@module} provides API for fetching data from a remote provider.
%%% @end
%%%-------------------------------------------------------------------
-module(rtransfer_server).
-author("Konrad Zemek").
-behavior(gen_server).

-include("modules/rtransfer/gateway.hrl").
-include("modules/rtransfer/rtransfer.hrl").
-include("modules/rtransfer/rt_container.hrl").

-define(RTRANSFER_RUNNING_JOBS, rtransfer_running_jobs).
-define(aggregators_map, aggregators_map).
-define(gateways_map, gateways_map).

%% API
-export([init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Initialize the module, starting all necessary services and side-effects.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: rtransfer:opt()) ->
    {ok, State :: rtransfer:opt()} |
    {ok, State :: rtransfer:opt(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(RtransferOpts) ->
    {ok, _} = rt_map:new({local, ?aggregators_map}),
    {ok, _} = rt_map:new({local, ?gateways_map}),
    {ok, RtransferOpts}.


%%--------------------------------------------------------------------
%% @doc
%% Handles transfer requests from external modules.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: rtransfer:opt()) ->
    {noreply, NewState :: rtransfer:opt()} |
    {noreply, NewState :: rtransfer:opt(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: rtransfer:opt()}.
handle_cast(#request_transfer{} = Request, State) ->
    FetchRetryNumber = proplists:get_value(retry, State, 5),

    #request_transfer{file_id = FileId, offset = Offset, size = Size,
        provider_id = ProviderId} = Request,

    Aggregator = spawn(fun() -> aggregator(Request, 0) end),

    Remote = provider_id_to_remote(ProviderId, State),

    {ok, ExistingBlocks} = rt_map:get(?gateways_map, FileId, Offset, Size),
    BaseBlock = #rt_block{file_id = FileId, offset = Offset, size = Size,
        provider_ref = ProviderId, terms = []},

    Blocks = rt_utils:partition(ExistingBlocks, BaseBlock),

    lists:foreach(
        fun(#rt_block{file_id = BFileId, offset = BOffset, size = BSize, terms = Nodes} = Block) ->
            {AdditionalNotify, GatewayNodes} =
                case Nodes of
                    [] ->
                        Node = pick_gw_node(),
                        rt_map:put(?gateways_map, Block#rt_block{terms = [Node]}),
                        {[self()], [Node]};

                    _  ->
                        {[], Nodes}
                end,

            rt_map:put(?aggregators_map, Block#rt_block{terms = [Aggregator]}),

            FetchRequest = #gw_fetch{file_id = BFileId, offset = BOffset, size = BSize,
                remote = Remote, notify = [Aggregator | AdditionalNotify], retry = FetchRetryNumber},

            gen_server:abcast(GatewayNodes, gateway, FetchRequest)
        end, Blocks),

    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% Handles status updates from gateway processes.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: rtransfer:opt()) ->
    {noreply, NewState :: rtransfer:opt()} |
    {noreply, NewState :: rtransfer:opt(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: rtransfer:opt()}.
handle_info({fetch_complete, 0, #gw_fetch{} = Action}, State) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size} = Action,
    rt_map:remove(?gateways_map, FileId, Offset, Size),
    retry(no_error, Action#gw_fetch{retry = Action#gw_fetch.retry - 1}, State),
    {noreply, State};

handle_info({fetch_complete, BytesRead, #gw_fetch{} = Action}, State) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size} = Action,
    rt_map:remove(?gateways_map, FileId, Offset, Size),
    rt_map:remove(?aggregators_map, FileId, Offset, BytesRead),
    retry(no_error, Action, State),
    {noreply, State};

handle_info({fetch_error, Details, #gw_fetch{} = Action}, State) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size} = Action,
    rt_map:remove(?gateways_map, FileId, Offset, Size),
    retry({error, Details}, Action#gw_fetch{retry = Action#gw_fetch.retry - 1}, State),
    {noreply, State}.


handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retries an action or cancels all aggregators waiting for the action
%% to finish if the number of retries has been exhausted.
%% @end
%%--------------------------------------------------------------------
-spec retry(Why :: no_error | {error, Reason :: term()}, Action :: #gw_fetch{},
    State :: rtransfer:opt()) -> ok.
retry(Why, #gw_fetch{retry = Retry, file_id = FileId, offset = Offset, size = Size}, _State) when Retry < 0 ->
    {ok, UnfinishedBlocks} = rt_map:get(?aggregators_map, FileId, Offset, Size),
    lists:foreach(
        fun(#rt_block{terms = Aggregators}) ->
            [Aggregator ! {stop, Why} || Aggregator <- Aggregators]
        end, UnfinishedBlocks),
    ok;

retry(_Why, #gw_fetch{file_id = FileId, offset = Offset, size = Size, retry = Retry}, State) ->
    {ok, UnfinishedBlocks} = rt_map:get(?aggregators_map, FileId, Offset, Size),

    lists:foreach(
        fun(#rt_block{file_id = BFileId, offset = BOffset, size = BSize, provider_ref = ProviderId, terms = Aggregators} = Block) ->
            Remote = provider_id_to_remote(ProviderId, State),
            Node = pick_gw_node(),
            rt_map:put(?gateways_map, Block#rt_block{terms = [Node]}),
            FetchRequest = #gw_fetch{file_id = BFileId, offset = BOffset, size = BSize,
                remote = Remote, notify = [self() | Aggregators], retry = Retry},
            gen_server:cast({gateway, Node}, FetchRequest)
        end, UnfinishedBlocks),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Pick one of gateway nodes available to perform requests.
%% @end
%%--------------------------------------------------------------------
-spec pick_gw_node() -> node().
pick_gw_node() ->
    node().
%%     Nodes = [node() | nodes()],
%%     NodeNo = random:uniform(length(Nodes)),
%%     lists:nth(NodeNo, Nodes).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translate provider's id to its TCP address.
%% @end
%%--------------------------------------------------------------------
-spec provider_id_to_remote(ProviderId :: binary(), State :: rtransfer:opt()) ->
    {inet:ip_address(), inet:port_number()}.
provider_id_to_remote(ProviderId, State) ->
    GetNodes = proplists:get_value(get_nodes_fun, State),
    Nodes = GetNodes(ProviderId),
    NodeNo = random:uniform(length(Nodes)),
    lists:nth(NodeNo, Nodes).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% A dedicated process to aggregate transfer updates, notifying client
%% after completion of all of the parts.
%% @end
%%--------------------------------------------------------------------
-spec aggregator(#request_transfer{}, Read :: non_neg_integer()) -> ok.
aggregator(#request_transfer{on_complete = OnComplete, size = Size} = Ref, Read) when Read >= Size ->
    OnComplete(Ref, {ok, Read}),
    ok;

aggregator(#request_transfer{notify = Notify, on_complete = OnComplete, offset = Offset} = Ref, Read) ->
    receive
        {stop, no_error} ->
            OnComplete(Ref, {ok, Read}),
            ok;

        {stop, {error, Reason}} ->
            OnComplete(Ref, {error, {other, Reason}}),
            ok;

        {fetch_complete, Num, #gw_fetch{offset = O}}
            when O =< Offset + Read andalso O + Num > Offset + Read ->

            NewEnd = O + Num,
            NewRead = NewEnd - Offset,
            Notify(Ref, O, Num),
            aggregator(Ref, NewRead)
    end.
