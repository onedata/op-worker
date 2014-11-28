%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% remote transfer manager functionality (gateways' management).
%% @end
%% ===================================================================

-module(rtransfer).
-behaviour(worker_plugin_behaviour).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/rtransfer/rtransfer.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").


-define(RTRANSFER_RUNNING_JOBS, rtransfer_running_jobs).
-define(aggregators_map, aggregators_map).
-define(gateways_map, gateways_map).
-define(rtransfer_tab, rtransfer_tab).

-export([init/1, handle/2, cleanup/0]).

-ifdef(TEST).
-compile(export_all).
-endif.


%% ====================================================================
%% API functions
%% ====================================================================


%% init/1
%% ====================================================================
%% @doc Initialize the module, starting all necessary services and side-effects.
%% @see worker_plugin_behaviour
-spec init(Args :: term()) -> ok | {error, Error :: any()}.
init(_Args) ->
    ets:new(?rtransfer_tab, [public, named_table, {read_concurrency, true}]),
    {ok, _} = rt_map:new({local, ?aggregators_map}),
    {ok, _} = rt_map:new({local, ?gateways_map}),

    spawn(
        fun() ->
            try
                Nodes = gen_server:call({global, central_cluster_manager}, get_nodes),
                ets:insert(?rtransfer_tab, {nodes, array:from_list(Nodes)})
            catch
                Error:Reason ->
                    ?error_stacktrace("~p:~p Failed to fetch nodes list from CCM: ~p:~p",
                        [?MODULE, ?LINE, Error, Reason])
            end
        end),

    ok.


%% handle/2
%% ====================================================================
%% @doc Handles transfer requests from external modules and status updates
%% from gateway processes.
%% @end
%% @see worker_plugin_behaviour
-spec handle(ProtocolVersion :: term(), Request :: term()) ->
    {ok, Ans :: term()} | {error, Error :: any()}.
handle(ProtocolVersion, #request_transfer{} = Request) ->
    {ok, FetchRetryNumber} = application:get_env(?APP_Name, rtransfer_fetch_retry_number),

    #request_transfer{file_id = FileId, offset = Offset, size = Size,
        provider_id = ProviderId, notify = Notify} = Request,

    Aggregator = spawn(fun() -> aggregator(
        fun(Msg) -> Notify ! Msg end, FileId, Offset, Size, 0) end),

    Remote = provider_id_to_remote(ProviderId),

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
                        {[{rtransfer, node()}], [Node]};

                    _  ->
                        {[], Nodes}
                end,

            rt_map:put(?aggregators_map, Block#rt_block{terms = [Aggregator]}),

            FetchRequest = #gw_fetch{file_id = BFileId, offset = BOffset, size = BSize,
                remote = Remote, notify = [Aggregator | AdditionalNotify], retry = FetchRetryNumber},

            gen_server:abcast(GatewayNodes, gateway, {asynch, ProtocolVersion, FetchRequest})
        end, Blocks),

    ok;

handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(ProtocolVersion, {fetch_complete, 0, #gw_fetch{} = Action}) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size} = Action,
    rt_map:remove(?gateways_map, FileId, Offset, Size),
    retry(ProtocolVersion, no_error, Action#gw_fetch{retry = Action#gw_fetch.retry - 1}),
    ok;

handle(ProtocolVersion, {fetch_complete, BytesRead, #gw_fetch{} = Action}) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size} = Action,
    rt_map:remove(?gateways_map, FileId, Offset, Size),
    rt_map:remove(?aggregators_map, FileId, Offset, BytesRead),
    retry(ProtocolVersion, no_error, Action),
    ok;

handle(ProtocolVersion, {fetch_error, Details, #gw_fetch{} = Action}) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size} = Action,
    rt_map:remove(?gateways_map, FileId, Offset, Size),
    retry(ProtocolVersion, {error, Details}, Action#gw_fetch{retry = Action#gw_fetch.retry - 1}),
    ok;

handle(_ProtocolVersion, _Msg) ->
    ?warning("Wrong request: ~p", [_Msg]),
	ok.


%% cleanup/0
%% ====================================================================
%% @doc Cleanup any state associated with the module.
%% @see worker_plugin_behaviour
-spec cleanup() -> ok | {error, Error :: any()}.
cleanup() ->
	ok.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% retry/2
%% ====================================================================
%% @doc Retries an action or cancells all aggregators waiting for the action
%% to finish if the number of retries has been exhausted.
-spec retry(ProtocolVersion :: term(), Why :: no_error | {error, Reason :: term()}, Action :: #gw_fetch{}) -> ok.
retry(_ProtocolVersion, Why, #gw_fetch{retry = Retry, file_id = FileId, offset = Offset, size = Size}) when Retry < 0 ->
    {ok, UnfinishedBlocks} = rt_map:get(?aggregators_map, FileId, Offset, Size),
    lists:foreach(
        fun(#rt_block{terms = Aggregators}) ->
            lists:foreach(fun(Aggregator) -> Aggregator ! {stop, Why} end, Aggregators)
        end, UnfinishedBlocks),
    ok;

retry(ProtocolVersion, _Why, #gw_fetch{file_id = FileId, offset = Offset, size = Size, retry = Retry}) ->
    {ok, UnfinishedBlocks} = rt_map:get(?aggregators_map, FileId, Offset, Size),

    lists:foreach(
        fun(#rt_block{file_id = BFileId, offset = BOffset, size = BSize, provider_ref = ProviderId, terms = Aggregators} = Block) ->
            Remote = provider_id_to_remote(ProviderId),
            Node = pick_gw_node(),
            rt_map:put(?gateways_map, Block#rt_block{terms = [Node]}),
            FetchRequest = #gw_fetch{file_id = BFileId, offset = BOffset, size = BSize,
                remote = Remote, notify = [{rtransfer, node()} | Aggregators], retry = Retry},
            gen_server:cast({gateway, Node}, {asynch, ProtocolVersion, FetchRequest})
        end, UnfinishedBlocks),
    ok.


%% pick_gw_node/0
%% ====================================================================
%% @doc Pick one of gateway nodes available to perform requests.
-spec pick_gw_node() -> node().
pick_gw_node() ->
    [{_, Nodes}] = ets:lookup(?rtransfer_tab, nodes),
    NodeNo = random:uniform(array:size(Nodes)) - 1,
    array:get(NodeNo, Nodes).


%% provider_id_to_remote/1
%% ====================================================================
%% @doc Translate provider's id to its TCP address.
%% @todo GR needs to provide more information, including gateway port
%% @todo Response cache should sometimes be cleared
-spec provider_id_to_remote(ProviderId :: binary()) -> {inet:ip_address(), inet:port_number()}.
provider_id_to_remote(ProviderId) ->
    {ok, GwPort} = application:get_env(?APP_Name, gateway_listener_port),

    URLs =
        case ets:lookup(?rtransfer_tab, {provider_addr_cache, ProviderId}) of
            [{_, ProviderURLs}] -> ProviderURLs;
            [] ->
                {ok, ProviderDetails} = gr_providers:get_details(provider, ProviderId),
                ProviderURLs =
                    array:from_list(
                        lists:map(
                            fun(URL) -> inet:parse_address(utils:ensure_list(URL)) end,
                            ProviderDetails#provider_details.urls)),

                ets:insert(?rtransfer_tab, {{provider_addr_cache, ProviderId}, ProviderURLs}),
                ProviderURLs
        end,

    URLNo = random:uniform(array:size(URLs)) - 1,
    {ok, URL} = array:get(URLNo, URLs),
    {URL, GwPort}.


%% aggregator/5
%% ====================================================================
%% @doc A dedicated process to aggregate transfer updates, notifying client
%% after completion of all of the parts.
%% @end
-spec aggregator(Notify :: function(), FileId :: list(), Offset :: non_neg_integer(),
                 Size :: pos_integer(), Read :: non_neg_integer()) -> ok.
aggregator(Notify, FileId, Offset, Size, Read) when Read >= Size ->
    Notify({transfer_complete, Read, {FileId, Offset, Size}}),
    ok;

aggregator(Notify, FileId, Offset, Size, Read) ->
    receive
        {stop, no_error} ->
            Notify({transfer_complete, Read, {FileId, Offset, Size}}),
            ok;

        {stop, {error, Reason}} ->
            Notify({transfer_error, Reason, {FileId, Offset, Size}}),
            ok;

        {fetch_complete, Num, #gw_fetch{offset = O}}
                when O =< Offset + Read andalso O + Num > Offset + Read ->

            NewEnd = O + Num,
            NewRead = NewEnd - Offset,
            aggregator(Notify, FileId, Offset, Size, NewRead)
    end.
