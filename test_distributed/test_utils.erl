%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains functions useful for distributed tests
%% e.g. function that sets environment before start, function that
%% synchronizes virtual machines etc.
%% @end
%% ===================================================================

-module(test_utils).

-include("registered_names.hrl").
-include("test_utils.hrl").
-include("modules_and_args.hrl").

-define(VIEW_REBUILDING_TIME, 2000).
-define(FUSE_SESSION_EXP_TIME, 8000).
-define(REQUEST_HANDLING_TIME, 1000).

%% Functions to use instead of timer
-export([ct_mock/4, wait_for_cluster_cast/0, wait_for_cluster_cast/1, wait_for_nodes_registration/1, wait_for_cluster_init/0,
         wait_for_cluster_init/1, wait_for_state_loading/0, wait_for_db_reaction/0, wait_for_fuse_session_exp/0, wait_for_request_handling/0]).

-export([add_user/4, add_user/5]).


%% add_user/4
%% ====================================================================
%% @doc Creates user with given Login, Cert path (for DN identification) and list of Spaces.
%%      First space on Spaces will be used as user's default space.
%%      Config shall be a proplist with at least {nodes, Nodes :: list()} entry.
%%      Returns #db_document with #user record that was just created or fails with exception.
%% @end
-spec add_user(Config :: list(), Login :: string(), Cert :: string(), Spaces :: [string() | binary()]) ->
    #db_document{} | no_return().
%% ====================================================================
add_user(Config, Login, Cert, Spaces) ->
    add_user(Config, Login, Cert, Spaces, <<"access_token">>).


%% add_user/5
%% ====================================================================
%% @doc Same as add_user/4 but also allows to explicitly set AccessToken for created user.
%% @end
-spec add_user(Config :: list(), Login :: string(), Cert :: string(), Spaces :: [string() | binary()], AccessToken :: binary()) ->
    #db_document{} | no_return().
%% ====================================================================
add_user(Config, Login, Cert, Spaces, AccessToken) ->

    [CCM | _] = ?config(nodes, Config),

    SpacesBinary = [utils:ensure_binary(Space) || Space <- Spaces],
    SpacesList = [utils:ensure_list(Space) || Space <- Spaces],

    {ReadFileAns, PemBin} = file:read_file(Cert),
    ?assertMatch({ok, _}, {ReadFileAns, PemBin}),
    {ExtractAns, RDNSequence} = rpc:call(CCM, user_logic, extract_dn_from_cert, [PemBin]),
    ?assertMatch({rdnSequence, _}, {ExtractAns, RDNSequence}),
    {ConvertAns, DN} = rpc:call(CCM, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
    ?assertMatch({ok, _}, {ConvertAns, DN}),

    DnList = [DN],
    Name = Login ++ " " ++ Login,
    Teams = SpacesList,
    Email = [Login ++ "@email.net"],

    rpc:call(CCM, user_logic, remove_user, [{dn, DN}]),

    AllSpaces = case get(ct_spaces) of
        undefined -> put(ct_spaces, SpacesBinary);
        Ctx -> put(ct_spaces, lists:usort(SpacesBinary ++ Ctx))
    end,

    {CreateUserAns, NewUserDoc} = rpc:call(CCM, user_logic, create_user, ["global_id_for_" ++ Login, Login, Name, Teams, Email, DnList, AccessToken]),
    ?assertMatch({ok, _}, {CreateUserAns, NewUserDoc}),

    test_utils:ct_mock(Config, gr_users, get_spaces, fun(_) -> {ok, #user_spaces{ids = SpacesBinary, default = lists:nth(1, SpacesBinary)}} end),
    test_utils:ct_mock(Config, gr_adapter, get_space_info, fun(SpaceId, _) -> {ok, #space_info{space_id = SpaceId, name = SpaceId, providers = [?LOCAL_PROVIDER_ID]}} end),

    test_utils:ct_mock(Config, gr_providers, get_spaces, fun(provider) -> {ok, AllSpaces} end),

    _UserDoc = rpc:call(CCM, user_logic, synchronize_spaces_info, [NewUserDoc, AccessToken]).


%% ct_mock/4
%% ====================================================================
%% @doc Evaluates meck:new(Module, [passthrough]) and meck:expect(Module, Method, Fun) on all
%%      cluster nodes from given test Config.
%%      For return value spac please see rpc:multicall/4.
%%      Config shall be a proplist with at least {nodes, Nodes :: list()} entry.
%% @end
-spec ct_mock(Config :: list(), Module :: atom(), Method :: atom(), Fun :: [term()]) ->
    {[term()], [term()]}.
%% ====================================================================
ct_mock(Config, Module, Method, Fun) ->
    NodesUp = ?config(nodes, Config),
    {_, []} = rpc:multicall(NodesUp, meck, new, [Module, [passthrough, non_strict, unstick, no_link]]),
    {_, []} = rpc:multicall(NodesUp, meck, expect, [Module, Method, Fun]).


%% wait_for_cluster_cast/0
%% ====================================================================
%% @doc Wait until cluster processes last cast.
%% @end
-spec wait_for_cluster_cast() -> ok | no_return().
%% ====================================================================
wait_for_cluster_cast() ->
  wait_for_cluster_cast({global, ?CCM}).

%% wait_for_cluster_cast/1
%% ====================================================================
%% @doc Wait until cluster processes last cast.
%% @end
-spec wait_for_cluster_cast(GenServ :: term()) -> ok | no_return().
%% ====================================================================
wait_for_cluster_cast(GenServ) ->
  timer:sleep(100),
  Ans = try
    gen_server:call(GenServ, check, 10000)
  catch
    E1:E2 ->
      {exception, E1, E2}
  end,
  ?assertEqual(ok, Ans).

%% wait_for_nodes_registration/1
%% ====================================================================
%% @doc Wait until all nodes will be registered.
%% @end
-spec wait_for_nodes_registration(NodesNum :: integer()) -> ok | no_return().
%% ====================================================================
wait_for_nodes_registration(NodesNum) ->
  wait_for_nodes_registration(NodesNum, 20).

%% wait_for_nodes_registration/2
%% ====================================================================
%% @doc Wait until all nodes will be registered.
%% @end
-spec wait_for_nodes_registration(NodesNum :: integer(), TriesNum :: integer()) -> ok | no_return().
%% ====================================================================
wait_for_nodes_registration(NodesNum, 0) ->
  ?assertEqual(NodesNum, check_nodes()),
  ok;

wait_for_nodes_registration(NodesNum, TriesNum) ->
  case check_nodes() of
    NodesNum -> ok;
    _ ->
      timer:sleep(500),
      wait_for_nodes_registration(NodesNum, TriesNum - 1)
  end.

%% check_nodes/0
%% ====================================================================
%% @doc Get number of registered nodes.
%% @end
-spec check_nodes() -> Ans when
  Ans :: integer() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
check_nodes() ->
  try
    length(gen_server:call({global, ?CCM}, get_nodes, 1000))
  catch
    E1:E2 ->
      {exception, E1, E2}
  end.

%% check_init/1
%% ====================================================================
%% @doc Check if cluster is initialized properly.
%% @end
-spec check_init(ModulesNum :: integer()) -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
check_init(ModulesNum) ->
  try
    {WList, StateNum} = gen_server:call({global, ?CCM}, get_workers, 1000),
    case length(WList) >= ModulesNum of
      true ->
        timer:sleep(500),
        Nodes = gen_server:call({global, ?CCM}, get_nodes, 1000),
        {_, CStateNum} = gen_server:call({global, ?CCM}, get_callbacks, 1000),
        CheckNode = fun(Node, TmpAns) ->
          StateNum2 = gen_server:call({?Dispatcher_Name, Node}, get_state_num, 1000),
          {_, CStateNum2} = gen_server:call({?Dispatcher_Name, Node}, get_callbacks, 1000),
          case (StateNum == StateNum2) and (CStateNum == CStateNum2) of
            true -> TmpAns;
            false -> false
          end
        end,
        lists:foldl(CheckNode, true, Nodes);
      false ->
        false
    end
  catch
    E1:E2 ->
      {exception, E1, E2}
  end.

%% wait_for_cluster_init/0
%% ====================================================================
%% @doc Wait until cluster is initialized properly.
%% @end
-spec wait_for_cluster_init() -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
wait_for_cluster_init() ->
  wait_for_cluster_init(0).

%% wait_for_cluster_init/1
%% ====================================================================
%% @doc Wait until cluster is initialized properly.
%% @end
-spec wait_for_cluster_init(ModulesNum :: integer()) -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
wait_for_cluster_init(ModulesNum) ->
  wait_for_cluster_init(ModulesNum + length(?Modules_With_Args), 20).

%% wait_for_cluster_init/2
%% ====================================================================
%% @doc Wait until cluster is initialized properly.
%% @end
-spec wait_for_cluster_init(ModulesNum :: integer(), TriesNum :: integer()) -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
wait_for_cluster_init(ModulesNum, 0) ->
  ?assert(check_init(ModulesNum));

wait_for_cluster_init(ModulesNum, TriesNum) ->
  case check_init(ModulesNum) of
    true -> true;
    _ ->
      timer:sleep(500),
      wait_for_cluster_init(ModulesNum, TriesNum - 1)
  end.

%% wait_for_db_reaction/0
%% ====================================================================
%% @doc Give DB time for processing request.
%% @end
-spec wait_for_db_reaction() -> ok.
%% ====================================================================
wait_for_db_reaction() ->
  timer:sleep(?VIEW_REBUILDING_TIME).

%% wait_for_fuse_session_exp/0
%% ====================================================================
%% @doc Give FUSE session time to expire.
%% @end
-spec wait_for_fuse_session_exp() -> ok.
%% ====================================================================
wait_for_fuse_session_exp() ->
  timer:sleep(?FUSE_SESSION_EXP_TIME).

%% wait_for_request_handling/0
%% ====================================================================
%% @doc Give cluster time for request handling.
%% @end
-spec wait_for_request_handling() -> ok.
%% ====================================================================
wait_for_request_handling() ->
  timer:sleep(?REQUEST_HANDLING_TIME).

%% check_state_loading/0
%% ====================================================================
%% @doc Check if state is loaded from DB.
%% @end
-spec check_state_loading() -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
check_state_loading() ->
  try
    gen_server:call({global, ?CCM}, check_state_loaded, 1000)
  catch
    E1:E2 ->
      {exception, E1, E2}
  end.

%% wait_for_state_loading/0
%% ====================================================================
%% @doc Wait until state is loaded from DB.
%% @end
-spec wait_for_state_loading() -> ok | no_return().
%% ====================================================================
wait_for_state_loading() ->
  wait_for_state_loading(20).

%% wait_for_state_loading/1
%% ====================================================================
%% @doc Wait until state is loaded from DB.
%% @end
-spec wait_for_state_loading(TriesNum :: integer()) -> ok | no_return().
%% ====================================================================
wait_for_state_loading(0) ->
  ?assert(check_state_loading());

wait_for_state_loading(TriesNum) ->
  case check_state_loading() of
    true -> true;
    _ ->
      timer:sleep(500),
      wait_for_state_loading(TriesNum - 1)
  end.