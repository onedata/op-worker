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
-include("oneprovider_modules/rtransfer/rt_heap.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

-export([init/1, handle/2, cleanup/0, real_init/0]).

-export([test/0]).
test() ->
    RequestTransfer = {request_transfer, "05073bf6703dee28bdb5016b3b53bf70", 0, 100, <<"050737e1f40ba94168bab38443c5c1e4">>, self()},
    gen_server:cast(rtransfer, {asynch, 1, RequestTransfer}),
    receive A -> A after timer:seconds(5) -> lol end.

%% ====================================================================
%% API functions
%% ====================================================================

init(_Args) ->
    ets:new(rtransfer, [public, named_table, {read_concurrency, true}]),
    timer:apply_after(timer:seconds(5), ?MODULE, real_init, []),
    ok.

handle(_ProtocolVersion, #request_transfer{} = Request) ->
    #request_transfer{file_id = FileId, offset = Offset, size = Size,
        provider_id = ProviderId, notify = Notify} = Request,

    Aggregator = spawn(fun() -> aggregator(
        fun(Msg) -> Notify ! Msg end, FileId, Offset, Size, 0) end),

    GwNode = pick_gw_node(),
    gen_server:cast({gateway, GwNode}, {asynch, 1, #gw_fetch{offset = Offset,
        size = Size, file_id = FileId,
        remote = provider_id_to_remote(ProviderId),
        notify = [{rtransfer, node()}, Aggregator]}});

handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, {fetch_complete, Num, #gw_fetch{}}) ->
    ok;

handle(_ProtocolVersion, {fetch_error, Details, #gw_fetch{}}) ->
    ok;

handle(_ProtocolVersion, _Msg) ->
    ?warning("Wrong request: ~p", [_Msg]),
	ok.

cleanup() ->
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

pick_gw_node() ->
    [{_, Nodes}] = ets:lookup(rtransfer, nodes),
    NodeNo = random:uniform(array:size(Nodes)) - 1,
    array:get(NodeNo, Nodes).

%% @todo GR needs to provide more information, including gateway port
provider_id_to_remote(ProviderId) ->
    {ok, GwPort} = application:get_env(?APP_Name, gateway_listener_port),
    {ok, ProviderDetails} = gr_providers:get_details(provider, ProviderId),
    Urls = array:from_list(ProviderDetails#provider_details.urls),
    UrlNo = random:uniform(array:size(Urls)) - 1,
    RemoteBin = array:get(UrlNo, Urls),
    {ok, Remote} = inet:parse_address(binary_to_list(RemoteBin)),
    {Remote, GwPort}.

aggregator(Notify, FileId, Offset, Size, Read) when Size >= Read ->
    Notify({ok, {FileId, Offset, Size}});

aggregator(Notify, FileId, Offset, Size, Read) ->
    receive
        {abort, Reason} ->
            Notify({error, Reason, {FileId, Offset, Size}});

        {fetch_complete, Num, #gw_fetch{offset = O}}
                when O =< Offset + Read andalso O + Num > Offset + Read ->

            NewEnd = O + Num,
            NewRead = NewEnd - Offset,
            aggregator(Notify, FileId, Offset, Size, NewRead)
    end.

real_init() ->
    Nodes = gen_server:call({global, central_cluster_manager}, get_nodes),
    ets:insert(rtransfer, {nodes, array:from_list(Nodes)}).