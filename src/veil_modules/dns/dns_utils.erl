%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is responsible for creating dns response for given dns request.
%% Currently only supported are queries of type a and class in.
%% @end
%% ===================================================================

-module(dns_utils).
-include_lib("kernel/src/inet_dns.hrl").
-include("veil_modules/dns/dns_utils.hrl").
-define(BASE_DNS_HEADER_SIZE, 12).  %% header size according to RFC1035#section-4.1.1
-define(BASE_DNS_QUESTION_SIZE, 4). %% query size according to RFC1035#section-4.1.2
-define(DNS_ANSWER_SIZE, 16).       %% making assumption that answer has matching query(label compression)
-define(MAX_UDP_PACKET, 512).


%% ====================================================================
%% API
%% ====================================================================
-export([generate_answer/5]).


%% generate_response/5
%% ====================================================================
%% @doc Generates binary dns response for given binary dns request, non protocol agnostic.
%% @end
%% ====================================================================
-spec generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL, Protocol) -> Result when
	Packet :: binary(),
	Dispatcher :: term(),
	DispatcherTimeout :: non_neg_integer(),
	ResponseTTL :: non_neg_integer(),
	Protocol :: udp | tcp,
	Result :: {ok, binary()} | {error, term()}.
%% ====================================================================
generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL, udp) ->
	case generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL) of
		{ok, Response} -> Header = Response#dns_rec.header,
						  AnList = Response#dns_rec.anlist,
						  QdList = Response#dns_rec.qdlist,
						  {NewHeader, FilteredAnList} = handle_max_udp_response_size(Header, QdList, AnList),
						  DNSRec = #dns_rec{header = NewHeader, anlist = FilteredAnList,
						                    arlist = [], nslist = [], qdlist = QdList},
						  {ok, inet_dns:encode(DNSRec)};
		Other -> Other
	end;

generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL, tcp) ->
	case generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL) of
		{ok, Response} -> {ok, inet_dns:encode(Response)};
		Other -> Other
	end.


%% handle_max_udp_response_size/3
%% ====================================================================
%% @doc Sets tc flag and truncates list of answers if packet size exceed limit.
%% @end
%% ====================================================================
-spec handle_max_udp_response_size(Header :: term(), QuestionList :: [#dns_query{domain::[any()]}], AnswerList :: list()) -> Result when
	Result :: {NewHeader :: term(), NewAnswerList :: list()}.
%% ====================================================================
handle_max_udp_response_size(Header, QuestionList, AnswerList) ->
	QueriesSize = lists:sum([?BASE_DNS_QUESTION_SIZE + length(Question#dns_query.domain) || Question <- QuestionList]),
	LeftSize = ?MAX_UDP_PACKET - ?BASE_DNS_HEADER_SIZE - QueriesSize,
	DNS_Responses_Size = length(AnswerList) * ?DNS_ANSWER_SIZE,
	if
		DNS_Responses_Size > LeftSize -> lager:info("Truncating dns response"),
									     FilteredAnswerList = lists:sublist(AnswerList, 1, erlang:max(LeftSize, 0) div ?DNS_ANSWER_SIZE),
										 {inet_dns:make_header(Header, tc, 1), FilteredAnswerList};
		true -> {Header, AnswerList}
	end.


%% generate_answer/4
%% ====================================================================
%% @doc Generate dns answer for given dns request - protocol agnostic.
%% @end
%% ====================================================================
-spec generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL) -> Result when
	Packet :: binary(),
	Dispatcher :: term(),
	DispatcherTimeout :: non_neg_integer(),
	ResponseTTL :: non_neg_integer(),
	Result :: term().
%% ====================================================================
generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL) ->
	case inet_dns:decode(Packet) of
		{ok, DNSRec} -> UpdatedHeader = inet_dns:make_header(DNSRec#dns_rec.header, ra, false),
						UpdatedHeaderWithQR = inet_dns:make_header(UpdatedHeader, qr, true),

						ArList = DNSRec#dns_rec.arlist,
						Queries = DNSRec#dns_rec.qdlist,

						A_In_Queries = [ Q || Q <- Queries, is_record(Q, dns_query),    %% Filters only supported queries
															Q#dns_query.type == ?S_A,
															Q#dns_query.class == in
						],

						Props = create_response_params(A_In_Queries, Queries, ArList, Dispatcher, DispatcherTimeout, ResponseTTL),
						create_response_from_properties(UpdatedHeaderWithQR, Queries, Props);
		Other -> Other
	end.

%% create_response_params/6
%% ====================================================================
%% @doc Returns all parameters required to construct dns_worker response.
%% @end
%% ====================================================================
-spec create_response_params(A_In_Queries, AllQueries, ArList, Dispatcher, DispatcherTimeout, ResponseTTL) -> Result when
	A_In_Queries :: list(),
	AllQueries :: list(),
	ArList :: list(),
	Dispatcher :: term(),
	DispatcherTimeout :: non_neg_integer(),
	ResponseTTL :: non_neg_integer(),
	Result :: list().
%% ====================================================================
create_response_params(A_In_Queries, AllQueries, ArList, Dispatcher, DispatcherTimeout, ResponseTTL) ->
	GenerateDispatcherAnswers = fun() ->
		lists:flatten([create_workers_response_params(Query, Dispatcher, DispatcherTimeout, ResponseTTL) || Query <- A_In_Queries])
	end,

	case {A_In_Queries, AllQueries, ArList} of
		{_, _, [_Head | _Tail]} -> [{rc, ?NOTIMP}];                 %% Extension Mechanisms for DNS are not implemented
																	%% For more info - RFC2671#section-5.3
		{_, [], _} -> [{rc, ?NOERROR}];                             %% No given queries
		{Q, Q, _} -> GenerateDispatcherAnswers();
		{_, _, _} -> [ {rc, ?NOTIMP} | GenerateDispatcherAnswers()] %% There are queries which we cannot handle
	end.

%% create_workers_response_params/4
%% ====================================================================
%% @doc Returns response params for specified query. Query labels comparisons are case insensitive.
%% @end
%% ====================================================================
-spec create_workers_response_params(Query, Dispatcher, DispatcherTimeout, ResponseTTL) -> Result when
	Query :: term(),
	Dispatcher :: term(),
	DispatcherTimeout :: non_neg_integer(),
	ResponseTTL :: non_neg_integer(),
	Result :: list().
%% ====================================================================
create_workers_response_params(#dns_query{domain = Domain,type = Type, class = Class}, Dispatcher, DispatcherTimeout, ResponseTTL) ->
	LoweredDomain = string:to_lower(Domain),
	[StrModule | Domain_Suffix] = string:tokens(LoweredDomain, "."),
	Module = list_to_atom(StrModule),

	case lists:member(Module, ?EXTERNALLY_VISIBLE_MODULES) of
		true ->
      DispatcherResponse = get_workers(Module, Dispatcher, DispatcherTimeout),
			translate_dispatcher_response_to_params(DispatcherResponse, Domain, Type, Class, ResponseTTL);
		false ->
      case Module of
        www ->
          DispatcherResponse2 = get_workers(control_panel, Dispatcher, DispatcherTimeout),
          translate_dispatcher_response_to_params(DispatcherResponse2, Domain, Type, Class, ResponseTTL);
        veilfs ->
          DispatcherResponse3 = get_workers(control_panel, Dispatcher, DispatcherTimeout),
          translate_dispatcher_response_to_params(DispatcherResponse3, Domain, Type, Class, ResponseTTL);
        _ ->
          [StrModule2 | _] = Domain_Suffix,
          case StrModule2 of
            "cluster" ->
              DispatcherResponse4 = get_nodes(Dispatcher, DispatcherTimeout),
              translate_dispatcher_response_to_params(DispatcherResponse4, Domain, Type, Class, ResponseTTL);
            _ ->
              [{rc, ?NXDOMAIN}]
          end
      end
	end.


%% get_workers/3
%% ====================================================================
%% @doc Returns workers for given module received from dns_worker.
%% @end
%% ====================================================================
-spec get_workers(Module, Dispatcher, DispatcherTimeout) -> Result when
	Module :: atom(),
	Dispatcher :: term(),
	DispatcherTimeout :: non_neg_integer(),
	Result :: {ok, list()} | {erorr, term()}.
%% ====================================================================
get_workers(Module, Dispatcher, DispatcherTimeout) ->
	try
		Pid = self(),
		DispatcherAns = gen_server:call(Dispatcher, {dns_worker, 1, Pid, {get_worker, Module}}),
		case DispatcherAns of
			ok -> receive
						{ok, ListOfIPs} -> {ok, ListOfIPs}
				  after
						DispatcherTimeout -> lager:error("Unexpected dispatcher timeout"), {error, timeout}
				  end;
			worker_not_found -> lager:error("Dispatcher error - worker not found"), {error, worker_not_found}
		end
	catch
		_:Error -> lager:error("Dispatcher not responding ~p", [Error]), {error, dispatcher_not_responding}
	end.

%% get_nodes/2
%% ====================================================================
%% @doc Returns nodes rom dns_worker.
%% @end
%% ====================================================================
-spec get_nodes(Dispatcher, DispatcherTimeout) -> Result when
  Dispatcher :: term(),
  DispatcherTimeout :: non_neg_integer(),
  Result :: {ok, list()} | {erorr, term()}.
%% ====================================================================
get_nodes(Dispatcher, DispatcherTimeout) ->
  try
    Pid = self(),
    DispatcherAns = gen_server:call(Dispatcher, {dns_worker, 1, Pid, get_nodes}),
    case DispatcherAns of
      ok -> receive
              {ok, ListOfIPs} -> {ok, ListOfIPs}
            after
              DispatcherTimeout -> lager:error("Unexpected dispatcher timeout"), {error, timeout}
            end;
      worker_not_found -> lager:error("Dispatcher error - worker not found"), {error, worker_not_found}
    end
  catch
    _:Error -> lager:error("Dispatcher not responding ~p", [Error]), {error, dispatcher_not_responding}
  end.

%% translate_dispatcher_response_to_params/5
%% ====================================================================
%% @doc Returns apropriate parameters based on dispatcher response.
%% @end
%% ====================================================================
-spec translate_dispatcher_response_to_params(DispatcherResponse, Domain, Type, Class, ResponseTTL) -> list() when
	DispatcherResponse :: {ok, ListOfWorkers :: list()} | {error, Reason :: atom()},
	Domain :: string(),
	Type :: ?T_A,
	Class :: ?C_IN,
	ResponseTTL :: non_neg_integer().
%% ====================================================================
translate_dispatcher_response_to_params(DispatcherResponse, Domain, Type, Class, ResponseTTL) ->
	Default_Worker_Parameters = [{domain, Domain}, {type, Type}, {ttl, ResponseTTL}, {class, Class}],

	case DispatcherResponse of
		{ok, Workers} ->
				[{aa, true} | [{an, [{data, Worker} | Default_Worker_Parameters]} || Worker <- Workers]];
		{error, _Reason} -> [{rc, ?SERVFAIL}]
	end.


%% create_response_from_properties/3
%% ====================================================================
%% @doc Creates dns response from given properties.
%% @end
%% ====================================================================
-spec create_response_from_properties(Header, Queries, Props) -> Result when
	Header :: term(),
	Queries :: term(),
	Props :: list(),
	Result :: {ok, term()}.
%% ====================================================================
create_response_from_properties(Header, Queries, Props) ->
	An = proplists:get_all_values(an, Props),
	AnRecords = [inet_dns:make_rr(Entry) || Entry <- An],

	AA = proplists:get_value(aa, Props, 0),
	UpdatedHeaderWithAA = inet_dns:make_header(Header, aa, AA),

	Rc = proplists:get_value(rc, Props, ?NOERROR),
	UpdatedHeaderWithRC = inet_dns:make_header(UpdatedHeaderWithAA, rcode, Rc),

	DNS_Rec = #dns_rec{header = UpdatedHeaderWithRC, arlist = [], qdlist = Queries, anlist = AnRecords, nslist = []},
	{ok, DNS_Rec}.