%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements {@link worker_plugin_behaviour} and
%% manages a DNS server module.
%% In addition, it implements {@link dns_query_handler_behaviour} -
%% DNS query handling logic.
%% @end
%% ===================================================================
-module(dns_worker).
-behaviour(worker_plugin_behaviour).
-behaviour(dns_query_handler_behaviour).

-include("oneprovider_modules/dns/dns_worker.hrl").
-include("registered_names.hrl").
-include("supervision_macros.hrl").
-include_lib("ctool/include/logging.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
%% worker_plugin_behaviour API
-export([init/1, handle/2, cleanup/0]).

%% dns_query_handler_behaviour API
-export([handle_a/1, handle_ns/1, handle_cname/1, handle_soa/1, handle_wks/1, handle_ptr/1, handle_hinfo/1, handle_minfo/1, handle_mx/1, handle_txt/1]).

%% ===================================================================
%% worker_plugin_behaviour API
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1.
%% @end
%% ====================================================================
-spec init(Args :: term()) -> Result when
    Result :: #dns_worker_state{} | {error, Error},
    Error :: term().
%% ====================================================================
init([]) ->
    {ok, DNSPort} = application:get_env(?APP_Name, dns_port),
    {ok, DNSResponseTTL} = application:get_env(?APP_Name, dns_response_ttl),
    {ok, EdnsMaxUdpSize} = application:get_env(?APP_Name, edns_max_udp_size),
    {ok, TCPNumAcceptors} = application:get_env(?APP_Name, dns_tcp_acceptor_pool_size),
    {ok, TCPTImeout} = application:get_env(?APP_Name, dns_tcp_timeout),
    OnFailureFun = fun() ->
        gen_server:cast({global, ?CCM}, {stop_worker, node(), ?MODULE}),
        ?error("~p is terminating", [?MODULE])
    end,
    ?info("Starting DNS server..."),
    case dns_server:start(?Supervisor_Name, DNSPort, dns_worker, DNSResponseTTL, EdnsMaxUdpSize, TCPNumAcceptors, TCPTImeout, OnFailureFun) of
        ok ->
            ok;
        Error ->
            ?error("Cannot start DNS server - ~p", Error),
            OnFailureFun()
    end,
    #dns_worker_state{};

init(InitialState) when is_record(InitialState, dns_worker_state) ->
    InitialState;

init(test) ->
    #dns_worker_state{};

init(_) ->
    throw(unknown_initial_state).


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% Calling handle(_, ping) returns pong.
%% Calling handle(_, get_version) returns current version of application.
%% Calling handle(_. {update_state, _}) updates plugin state.
%% Calling handle(_, {get_worker, Name}) returns list of ipv4 addresses of workers with specified name.
%% @end
%% ====================================================================
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version |
    {update_state, list(), list()} |
    {get_worker, atom()} |
    get_nodes,
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Response :: [inet:ip4_address()],
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, {update_state, ModulesToNodes, NLoads, AvgLoad}) ->
    ?info("DNS state update: ~p", [{ModulesToNodes, NLoads, AvgLoad}]),
    try
        ModulesToNodes2 = lists:map(fun({Module, Nodes}) ->
            GetLoads = fun({Node, V}) ->
                {Node, V, V}
            end,
            {Module, lists:map(GetLoads, Nodes)}
        end, ModulesToNodes),
        New_DNS_State = #dns_worker_state{workers_list = ModulesToNodes2, nodes_list = NLoads, avg_load = AvgLoad},
        case gen_server:call(?MODULE, {updatePlugInState, New_DNS_State}) of
            ok ->
                ok;
            UpdateError ->
                ?error("DNS update error: ~p", [UpdateError]),
                udpate_error
        end
    catch
        E1:E2 ->
            ?error("DNS update error: ~p:~p", [E1, E2]),
            udpate_error
    end;

handle(_ProtocolVersion, {get_worker, Module}) ->
    try
        DNSState = gen_server:call(?MODULE, getPlugInState),
        WorkerList = DNSState#dns_worker_state.workers_list,
        NodesList = DNSState#dns_worker_state.nodes_list,
        Result = proplists:get_value(Module, WorkerList, []),

        PrepareResult = fun({Node, V, C}, {TmpAns, TmpWorkers}) ->
            TmpAns2 = case C of
                          V -> [Node | TmpAns];
                          _ -> TmpAns
                      end,
            C2 = case C of
                     1 -> V;
                     _ -> C - 1
                 end,
            {TmpAns2, [{Node, V, C2} | TmpWorkers]}
        end,
        {Result2, ModuleWorkerList} = lists:foldl(PrepareResult, {[], []}, Result),

        PrepareState = fun({M, Workers}, TmpWorkersList) ->
            case M =:= Module of
                true -> [{M, ModuleWorkerList} | TmpWorkersList];
                false -> [{M, Workers} | TmpWorkersList]
            end
        end,
        NewWorkersList = lists:foldl(PrepareState, [], WorkerList),

        New_DNS_State = DNSState#dns_worker_state{workers_list = NewWorkersList},

        case gen_server:call(?MODULE, {updatePlugInState, New_DNS_State}) of
            ok ->
                random:seed(now()),
                Result3 = make_ans_random(Result2),
                case Module of
                    control_panel ->
                        {ok, Result3};
                    _ ->
                        create_ans(Result3, NodesList)
                end;
            UpdateError ->
                ?error("DNS get_worker error: ~p", [UpdateError]),
                {error, dns_update_state_error}
        end
    catch
        E1:E2 ->
            ?error("DNS get_worker error: ~p:~p", [E1, E2]),
            {error, dns_get_worker_error}
    end;

handle(_ProtocolVersion, get_nodes) ->
    try
        DNSState = gen_server:call(?MODULE, getPlugInState),
        NodesList = DNSState#dns_worker_state.nodes_list,
        AvgLoad = DNSState#dns_worker_state.avg_load,

        case AvgLoad of
            0 ->
                Res = make_ans_random(lists:map(
                    fun({Node, _}) ->
                        Node
                    end, NodesList)),
                {ok, Res};
            _ ->
                random:seed(now()),
                ChooseNodes = fun({Node, NodeLoad}, TmpAns) ->
                    case is_number(NodeLoad) and (NodeLoad > 0) of
                        true ->
                            Ratio = AvgLoad / NodeLoad,
                            case Ratio >= random:uniform() of
                                true ->
                                    [Node | TmpAns];
                                false ->
                                    TmpAns
                            end;
                        false ->
                            [Node | TmpAns]
                    end
                end,
                Result = lists:foldl(ChooseNodes, [], NodesList),
                Result2 = make_ans_random(Result),
                create_ans(Result2, NodesList)
        end
    catch
        E1:E2 ->
            ?error("DNS get_nodes error: ~p:~p", [E1, E2]),
            {error, get_nodes}
    end;

handle(ProtocolVersion, Msg) ->
    ?warning("Wrong request: ~p", [Msg]),
    throw({unsupported_request, ProtocolVersion, Msg}).

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%% ====================================================================
-spec cleanup() -> Result when
    Result :: ok.
%% ====================================================================
cleanup() ->
    dns_server:stop(?Supervisor_Name).


%% create_ans/2
%% ====================================================================
%% @doc Creates answer from results list (adds additional node if needed).
%% @end
%% ====================================================================
-spec create_ans(Result :: list(), NodesList :: list()) -> Ans when
    Ans :: {ok, IPs},
    IPs :: list().
%% ====================================================================
create_ans(Result, NodesList) ->
    case (length(Result) > 1) or (length(NodesList) =< 1) of
        true -> {ok, Result};
        false ->
            NodeNum = random:uniform(length(NodesList)),
            {NewNode, _} = lists:nth(NodeNum, NodesList),
            case Result =:= [NewNode] of
                true ->
                    {NewNode2, _} = lists:nth((NodeNum rem length(NodesList) + 1), NodesList),
                    {ok, Result ++ [NewNode2]};
                false ->
                    {ok, Result ++ [NewNode]}
            end
    end.


%% make_ans_random/1
%% ====================================================================
%% @doc Makes order of nodes in answer random.
%% @end
%% ====================================================================
-spec make_ans_random(Result :: list()) -> IPs when
    IPs :: list().
%% ====================================================================
make_ans_random(Result) ->
    Len = length(Result),
    case Len of
        0 -> [];
        1 -> Result;
        _ ->
            NodeNum = random:uniform(Len),
            NewRes = lists:sublist(Result, 1, NodeNum - 1) ++ lists:sublist(Result, NodeNum + 1, Len),
            [lists:nth(NodeNum, Result) | make_ans_random(NewRes)]
    end.


%% ===================================================================
%% dns_query_handler_behaviour API
%% ===================================================================

%% handle_a/1
%% ====================================================================
%% @doc Handles DNS queries of type A.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_a(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()}.
%% ====================================================================
handle_a(Domain) ->
    case parse_domain(Domain) of
        nx_domain ->
            nx_domain;
        {Prefix, _Suffix} ->
            if
                Prefix =:= "" ->
                    get_workers(control_panel);
                Prefix =:= "www" ->
                    get_workers(control_panel);
                Prefix =:= "cluster" ->
                    get_nodes();
                true ->
                    Module = try list_to_existing_atom(Prefix) catch _:_ -> undefined end,
                    case lists:member(Module, ?EXTERNALLY_VISIBLE_MODULES) of
                        true -> get_workers(Module);
                        false -> nx_domain
                    end
            end
    end.


%% handle_ns/1
%% ====================================================================
%% @doc Handles DNS queries of type NS.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_ns(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: string().
%% ====================================================================
handle_ns(Domain) ->
    case parse_domain(Domain) of
        nx_domain ->
            nx_domain;
        {Prefix, Suffix} ->
            case is_prefix_valid(Prefix) of
                false ->
                    nx_domain;
                true ->
                    {ok, [Suffix]}
            end
    end.


%% handle_cname/1
%% ====================================================================
%% @doc Handles DNS queries of type CNAME.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_cname(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: string().
%% ====================================================================
handle_cname(Domain) ->
    case parse_domain(Domain) of
        nx_domain ->
            nx_domain;
        {Prefix, Suffix} ->
            case is_prefix_valid(Prefix) of
                false ->
                    nx_domain;
                true ->
                    {ok, [Suffix]}
            end
    end.


%% handle_mx/1
%% ====================================================================
%% @doc Handles DNS queries of type MX.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_mx(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {Pref :: integer(), Exch :: string()}.
%% ====================================================================
handle_mx(Domain) ->
    case parse_domain(Domain) of
        nx_domain ->
            nx_domain;
        {Prefix, Suffix} ->
            case is_prefix_valid(Prefix) of
                false ->
                    nx_domain;
                true ->
                    {ok, [{0, Suffix}]}
            end
    end.


%% handle_soa/1
%% ====================================================================
%% @doc Handles DNS queries of type SOA.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_soa(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {MName :: string(), RName :: string(), Serial :: integer(), Refresh :: integer(),
    Retry :: integer(), Expiry :: integer(), Minimum :: integer()}.
%% ====================================================================
handle_soa(_Domain) -> not_impl.


%% handle_wks/1
%% ====================================================================
%% @doc Handles DNS queries of type WKS.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_wks(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {{A :: byte(), B :: byte(), C :: byte(), D :: byte()}, Proto :: string(), BitMap :: string()}.
%% ====================================================================
handle_wks(_Domain) -> not_impl.


%% handle_ptr1
%% ====================================================================
%% @doc Handles DNS queries of type PTR.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_ptr(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: string().
%% ====================================================================
handle_ptr(_Domain) -> not_impl.


%% handle_hinfo/1
%% ====================================================================
%% @doc Handles DNS queries of type HINFO.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_hinfo(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {CPU :: string(), OS :: string()}.
%% ====================================================================
handle_hinfo(_Domain) -> not_impl.


%% handle_minfo/1
%% ====================================================================
%% @doc Handles DNS queries of type MINFO.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_minfo(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {RM :: string(), EM :: string()}.
%% ====================================================================
handle_minfo(_Domain) -> not_impl.


%% handle_txt/1
%% ====================================================================
%% @doc Handles DNS queries of type TXT.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%% ====================================================================
-spec handle_txt(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: [string()].
%% ====================================================================
handle_txt(_Domain) -> not_impl.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% parse_domain/1
%% ====================================================================
%% @doc Split the domain name into prefix and suffix, where suffix matches the
%% canonical provider's hostname (retrieved from env). The split is made on the dot between prefix and suffix.
%% If that's not possible, returns nx_domain.
%% @end
%% ====================================================================
-spec parse_domain(Domain :: string()) -> {Prefix :: string(), Suffix :: string()} | nx_domain.
%% ====================================================================
parse_domain(Domain) ->
    {ok, ProviderHostnameWithoutDot} = application:get_env(?APP_Name, provider_hostname),
    case ProviderHostnameWithoutDot =:= Domain of
        true ->
            {"", ProviderHostnameWithoutDot};
        false ->
            ProviderHostname = "." ++ ProviderHostnameWithoutDot,
            HostNamePos = string:rstr(Domain, ProviderHostname),
            % If hostname is at this position, it's a suffix (the string ends with it)
            ValidHostNamePos = length(Domain) - length(ProviderHostname) + 1,
            case HostNamePos =:= ValidHostNamePos of
                false ->
                    nx_domain;
                true ->
                    {string:sub_string(Domain, 1, HostNamePos - 1), ProviderHostnameWithoutDot}
            end
    end.


%% is_prefix_valid/1
%% ====================================================================
%% @doc Convenience function to check if specific subdomain is existent.
%% @end
%% ====================================================================
-spec is_prefix_valid(Prefix :: string()) -> boolean().
%% ====================================================================
is_prefix_valid(Prefix) ->
    case Prefix of
        "" ->
            true;
        "www" ->
            true;
        "cluster" ->
            true;
        _ ->
            Module = try list_to_existing_atom(Prefix) catch _:_ -> undefined end,
            lists:member(Module, ?EXTERNALLY_VISIBLE_MODULES)
    end.


%% get_workers/1
%% ====================================================================
%% @doc Selects couple of nodes hosting given worker and returns their IPs.
%% @end
%% ====================================================================
-spec get_workers(Module :: atom()) -> {ok, list()} | serv_fail.
%% ====================================================================
get_workers(Module) ->
    try
        {ok, DispatcherTimeout} = application:get_env(?APP_Name, dispatcher_timeout),
        DispatcherAns = gen_server:call(?Dispatcher_Name, {dns_worker, 1, self(), {get_worker, Module}}),
        case DispatcherAns of
            ok -> receive
                      {ok, ListOfIPs} ->
                          {ok, ListOfIPs};
                      {error, Error} ->
                          ?error("Unexpected dispatcher error ~p", [Error]),
                          serv_fail
                  after
                      DispatcherTimeout ->
                          ?error("Unexpected dispatcher timeout"),
                          serv_fail
                  end;
            worker_not_found ->
                ?error("Dispatcher error - worker not found"),
                serv_fail
        end
    catch
        _:Error2 ->
            ?error("Dispatcher not responding ~p", [Error2]),
            serv_fail
    end.


%% get_nodes/0
%% ====================================================================
%% @doc Selects couple of nodes from the cluster and returns their IPs.
%% @end
%% ====================================================================
-spec get_nodes() -> {ok, list()} | serv_fail.
%% ====================================================================
get_nodes() ->
    try
        {ok, DispatcherTimeout} = application:get_env(?APP_Name, dispatcher_timeout),
        DispatcherAns = gen_server:call(?Dispatcher_Name, {dns_worker, 1, self(), get_nodes}),
        case DispatcherAns of
            ok -> receive
                      {ok, ListOfIPs} ->
                          {ok, ListOfIPs};
                      {error, Error} ->
                          ?error("Unexpected dispatcher error ~p", [Error]),
                          serv_fail
                  after
                      DispatcherTimeout ->
                          ?error("Unexpected dispatcher timeout"),
                          serv_fail
                  end;
            worker_not_found ->
                ?error("Dispatcher error - worker not found"),
                serv_fail
        end
    catch
        _:Error2 ->
            ?error("Dispatcher not responding ~p", [Error2]),
            serv_fail
    end.

