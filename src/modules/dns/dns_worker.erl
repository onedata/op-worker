%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and
%%% manages a DNS server module.
%%% In addition, it implements {@link dns_query_handler_behaviour} -
%%% DNS query handling logic.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).
-behaviour(dns_query_handler_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/dns/dns.hrl").

%% This record is used by dns_worker (it contains its state). The first element of
%% a tuple is a name of a module and the second is a list of ip addresses of nodes
%% sorted ascending by load.
%%
%% Example:
%%     Assuming that dns_worker plugin works on node1@127.0.0.1 and node2@192.168.0.1,
%%     (load on node1 < load on node2) and control_panel works on node3@127.0.0.1,
%%     dns_worker state will look like this:
%%     {dns_state, [{dns_worker, [{127,0,0,1}, {192,168,0,1}]}, {control_panel, [{127,0,0,1}]}]}
-record(dns_worker_state, {
    workers_list = [] :: [{atom(),  [{inet:ip4_address(), integer(), integer()}]}],
    nodes_list = [] :: [{inet:ip4_address(),  number()}],
    avg_load = 0 :: number()
}).

-define(EXTERNALLY_VISIBLE_MODULES, [http_worker, dns_worker]).
-define(HEALTHCHECK_TIMEOUT, timer:seconds(5)).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% dns_query_handler_behaviour callbacks
-export([handle_a/1, handle_ns/1, handle_cname/1, handle_soa/1, handle_wks/1,
    handle_ptr/1, handle_hinfo/1, handle_minfo/1, handle_mx/1, handle_txt/1]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, #dns_worker_state{}} | {error, Reason :: term()}.
init([]) ->
    {ok, #dns_worker_state{}};

init(InitialState) when is_record(InitialState, dns_worker_state) ->
    {ok, InitialState};

init(test) ->
    {ok, #dns_worker_state{}};

init(_) ->
    throw(unknown_initial_state).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request, State :: term()) -> Result when
    Request :: ping | healthcheck |
    {update_state, list(), list()} |
    {get_worker, atom()} |
    get_nodes,
    Result :: nagios_handler:healthcheck_reponse() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: [inet:ip4_address()],
    Reason :: term().
handle(ping, _) ->
    pong;

handle(healthcheck, _) ->
    healthcheck();

handle({update_state, ModulesToNodes, NLoads, AvgLoad}, _) ->
    ?info("DNS state update: ~p", [{ModulesToNodes, NLoads, AvgLoad}]),
    try
        ModulesToNodes2 = lists:map(fun({Module, Nodes}) ->
            GetLoads = fun({Node, V}) ->
                {Node, V, V}
            end,
            {Module, lists:map(GetLoads, Nodes)}
        end, ModulesToNodes),
        New_DNS_State = #dns_worker_state{workers_list = ModulesToNodes2, nodes_list = NLoads, avg_load = AvgLoad},
        case gen_server:call(?MODULE, {update_plugin_state, New_DNS_State}) of
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

handle({handle_a, Domain}, _) ->
    IPList = case parse_domain(Domain) of
                 unknown_domain ->
                     refused;
                 {Prefix, _Suffix} ->
                     % Accept all prefixes that consist of one part
                     case string:str(Prefix, ".") =:= 0 andalso length(Prefix) > 0 of
                         true ->
                             case Prefix of
                                 "cluster" ->
                                     % Return all nodes when asked about cluster
                                     get_nodes();
                                 _ ->
                                     % Check if query concers any specific module, if not assume it's a http request
                                     Module = try list_to_existing_atom(Prefix) catch _:_ -> undefined end,
                                     case lists:member(Module, ?EXTERNALLY_VISIBLE_MODULES) of
                                         false ->
                                             get_workers(http_worker);
                                         true ->
                                             get_workers(Module)
                                     end
                             end;
                         false ->
                             nx_domain
                     end
             end,
    case IPList of
        nx_domain ->
            nx_domain;
        serv_fail ->
            serv_fail;
        refused ->
            refused;
        _ ->
            {ok, TTL} = application:get_env(?APP_NAME, dns_a_response_ttl),
            {ok,
                    [dns_server:answer_record(Domain, TTL, ?S_A, IP) || IP <- IPList] ++
                    [dns_server:authoritative_answer_flag(true)]
            }
    end;

handle({handle_ns, Domain}, _) ->
    case parse_domain(Domain) of
        unknown_domain ->
            refused;
        {Prefix, _Suffix} ->
            % Accept all prefixes that consist of one part
            case string:str(Prefix, ".") =:= 0 andalso length(Prefix) > 0 of
                true ->
                    {ok, TTL} = application:get_env(?APP_NAME, dns_ns_response_ttl),
                    {ok,
                            [dns_server:answer_record(Domain, TTL, ?S_NS, inet_parse:ntoa(IP)) || IP <- get_nodes()] ++
                            [dns_server:authoritative_answer_flag(true)]
                    };
                false ->
                    nx_domain
            end
    end;

handle(_Request, _) ->
    ?log_bad_request(_Request),
    throw({unsupported_request, _Request}).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    dns_server:stop(?APPLICATION_SUPERVISOR_NAME).


%%%===================================================================
%%% dns_query_handler_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type A.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_a(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_a(Domain) ->
    call_dns_worker({handle_a, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type NS.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ns(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_ns(Domain) ->
    call_dns_worker({handle_ns, Domain}).

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type CNAME.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_cname(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_cname(_Domain) -> not_impl.

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MX.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_mx(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_mx(_Domain) -> not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type SOA.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_soa(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_soa(_Domain) -> not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type WKS.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_wks(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_wks(_Domain) -> not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type PTR.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ptr(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_ptr(_Domain) -> not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type HINFO.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_hinfo(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_hinfo(_Domain) -> not_impl.

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MINFO.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_minfo(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
%% ====================================================================
handle_minfo(_Domain) -> not_impl.

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type TXT.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_txt(Domain :: string()) -> {reply_type(), dns_query_handler_reponse()} | reply_type().
handle_txt(_Domain) -> not_impl.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Split the domain name into prefix and suffix, where suffix matches the
%% canonical globalregistry hostname (retrieved from env). The split is made on the dot between prefix and suffix.
%% If that's not possible, returns unknown_domain.
%% @end
%%--------------------------------------------------------------------
-spec parse_domain(Domain :: string()) -> {Prefix :: string(), Suffix :: string()} | unknown_domain.
parse_domain(DomainArg) ->
    % If requested domain starts with 'www.', ignore it
    Domain = case DomainArg of
                 [$w, $w, $w, $. | Rest] -> Rest;
                 Other -> Other
             end,
    {ok, ProviderHostnameWithoutDot} = application:get_env(?APP_NAME, global_registry_hostname),
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
                    unknown_domain;
                true ->
                    {string:sub_string(Domain, 1, HostNamePos - 1), ProviderHostnameWithoutDot}
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects couple of nodes hosting given worker and returns their IPs.
%% @end
%%--------------------------------------------------------------------
-spec get_workers(Module :: atom()) -> list() | serv_fail.
get_workers(Module) ->
    try
        DNSState = gen_server:call(?MODULE, get_plugin_state),
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

        case gen_server:call(?MODULE, {update_plugin_state, New_DNS_State}) of
            ok ->
                random:seed(now()),
                Result3 = make_ans_random(Result2),
                case Module of
                    http_worker ->
                        Result3;
                    _ ->
                        create_ans(Result3, NodesList)
                end;
            UpdateError ->
                ?error("DNS get_worker error: ~p", [UpdateError]),
                serv_fail
        end
    catch
        E1:E2 ->
            ?error("DNS get_worker error: ~p:~p", [E1, E2]),
            serv_fail
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects couple of nodes from the cluster and returns their IPs.
%% @end
%%--------------------------------------------------------------------
-spec get_nodes() -> list() | serv_fail.
get_nodes() ->
    try
        DNSState = gen_server:call(?MODULE, get_plugin_state),
        NodesList = DNSState#dns_worker_state.nodes_list,
        AvgLoad = DNSState#dns_worker_state.avg_load,

        case AvgLoad of
            0 ->
                Res = make_ans_random(lists:map(
                    fun({Node, _}) ->
                        Node
                    end, NodesList)),
                Res;
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
            serv_fail
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates answer from results list (adds additional node if needed).
%% @end
%%--------------------------------------------------------------------
-spec create_ans(Result :: list(), NodesList :: list()) -> IPs :: [term()].
create_ans(Result, NodesList) ->
    case (length(Result) > 1) or (length(NodesList) =< 1) of
        true -> Result;
        false ->
            NodeNum = random:uniform(length(NodesList)),
            {NewNode, _} = lists:nth(NodeNum, NodesList),
            case Result =:= [NewNode] of
                true ->
                    {NewNode2, _} = lists:nth((NodeNum rem length(NodesList) + 1), NodesList),
                    Result ++ [NewNode2];
                false ->
                    Result ++ [NewNode]
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%%  Makes order of nodes in answer random.
%% @end
%%--------------------------------------------------------------------
-spec make_ans_random(Result :: list()) -> IPs :: [term()].
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls dns_worker module gen_server with given request. Used to delegate
%% DNS query processing to dns_worker.
%% @end
%%--------------------------------------------------------------------
-spec call_dns_worker(Request :: term()) -> term().
call_dns_worker(Request) ->
    worker_proxy:call(dns_worker, Request).

%%--------------------------------------------------------------------
%% @doc
%% healthcheck dns endpoint
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, Reason :: atom()}.
healthcheck() ->
    {ok, DNSPort} = application:get_env(?APP_NAME, dns_port),
    Query = inet_dns:encode(
        #dns_rec{
            header = #dns_header{
                id = crypto:rand_uniform(1, 16#FFFF),
                opcode = 'query',
                rd = true
            },
            qdlist = [#dns_query{
                domain = "localhost",
                type = soa,
                class = in
            }],
            arlist = [{dns_rr_opt, ".", opt, 1280, 0, 0, 0, <<>>}]
        }),
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    gen_udp:send(Socket, "127.0.0.1", DNSPort, Query),
    case gen_udp:recv(Socket, 65535, ?HEALTHCHECK_TIMEOUT) of
        {ok, _} -> ok;
        _ -> {error, no_dns}
    end.