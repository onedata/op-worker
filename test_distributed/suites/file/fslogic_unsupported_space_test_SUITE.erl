%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of how fslogic_worker answers requests concerning a space this
%%% provider doesn't support: it serves the attributes of the space dir.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_unsupported_space_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    space_dir_attrs_are_served_test/1
]).

all() -> [
    space_dir_attrs_are_served_test
].

% supported by krakow only
-define(UNSUPPORTED_SPACE, space_krk_p).
-define(TEST_USER, user1).


%%%===================================================================
%%% Test functions
%%%===================================================================


space_dir_attrs_are_served_test(_Config) ->
    SpaceDirGuid = space_dir:guid(oct_background:get_space_id(?UNSUPPORTED_SPACE)),
    Sock = connect('ONES3_TYPE'),

    ?assertMatch({ok, #'FileAttr'{uuid = SpaceDirGuid}}, get_file_attr(Sock, SpaceDirGuid)),

    % an unhealthy local storage of another space changes nothing
    set_unhealthy_storages([<<"dummy_unhealthy_storage">>]),
    try
        ?assertMatch({ok, #'FileAttr'{uuid = SpaceDirGuid}}, get_file_attr(Sock, SpaceDirGuid))
    after
        set_unhealthy_storages([])
    end,

    ssl:close(Sock).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec connect(atom()) -> ssl:sslsocket().
connect(ClientType) ->
    {ok, {Sock, _SessId}} = fuse_test_utils:connect_via_token(
        oct_background:get_random_provider_node(paris), [{active, true}], str_utils:rand_hex(10),
        oct_background:get_user_access_token(?TEST_USER), ClientType
    ),
    Sock.


%% @private
-spec get_file_attr(ssl:sslsocket(), file_id:file_guid()) ->
    {ok, #'FileAttr'{}} | {error, atom()}.
get_file_attr(Sock, Guid) ->
    send_fuse_request(Sock, {file_request, #'FileRequest'{
        context_guid = Guid,
        file_request = {get_file_attr, #'GetFileAttr'{}}
    }}).


%% @private
-spec send_fuse_request(ssl:sslsocket(), tuple()) -> {ok, term()} | {error, atom()}.
send_fuse_request(Sock, FuseRequest) ->
    MsgId = integer_to_binary(erlang:unique_integer([positive])),
    ok = ssl:send(Sock, messages:encode_msg(#'ClientMessage'{
        message_id = MsgId,
        message_body = {fuse_request, #'FuseRequest'{fuse_request = FuseRequest}}
    })),
    #'ServerMessage'{message_body = {fuse_response, #'FuseResponse'{
        status = #'Status'{code = Code},
        fuse_response = Response
    }}} = fuse_test_utils:receive_server_message(),
    case Code of
        ok -> {ok, element(2, Response)};
        _ -> {error, Code}
    end.


%% @private
-spec set_unhealthy_storages([storage:id()]) -> ok.
set_unhealthy_storages(StorageIds) ->
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, worker_host, state_put, [fslogic_worker, unhealthy_storages, StorageIds])
    end, oct_background:get_provider_nodes(paris)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
