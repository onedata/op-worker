%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of how fslogic_worker answers requests concerning a space this
%%% provider doesn't support: it refuses them and serves the attributes of
%%% the space dir only.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_unsupported_space_test_SUITE).
-author("Bartosz Walkowicz").

-include("env/space_setup_utils.hrl").
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
    space_dir_attrs_are_served_test/1,
    other_requests_are_refused_test/1,
    requests_rooted_at_space_dir_are_refused_test/1
]).

all() -> [
    space_dir_attrs_are_served_test,
    other_requests_are_refused_test,
    requests_rooted_at_space_dir_are_refused_test
].

% supported by krakow only
-define(UNSUPPORTED_SPACE, space_krk_p).
-define(TEST_USER, user1).

-define(ATTEMPTS, 60).


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


other_requests_are_refused_test(_Config) ->
    SpaceDirGuid = space_dir:guid(oct_background:get_space_id(?UNSUPPORTED_SPACE)),
    SpacePath = <<"/", (atom_to_binary(?UNSUPPORTED_SPACE))/binary>>,
    {ok, FileGuid} = ?assertMatch({ok, _}, rpc:call(
        oct_background:get_random_provider_node(krakow), lfm, create,
        [oct_background:get_user_session_id(?TEST_USER, krakow), SpaceDirGuid, generator:gen_name(), 8#644]
    )),

    lists:foreach(fun({ClientType, ExpCode}) ->
        Sock = connect(ClientType),
        ?assertEqual({error, ExpCode}, get_file_attr(Sock, FileGuid)),
        ?assertEqual({error, ExpCode}, get_children_attrs(Sock, SpaceDirGuid)),
        ?assertEqual({error, ExpCode}, resolve_guid(Sock, SpacePath)),
        ssl:close(Sock)
    end, [
        {'ONECLIENT_TYPE', einval},
        {'ONES3_TYPE', einval}
    ]).


% A provider whose support was just revoked may still hold permissions that let
% a request through, so the refusal must not depend on them.
requests_rooted_at_space_dir_are_refused_test(_Config) ->
    ParisNode = oct_background:get_random_provider_node(paris),
    SessId = oct_background:get_user_session_id(?TEST_USER, paris),

    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = ?FUNCTION_NAME,
        owner = ?TEST_USER,
        % an imported storage of the scenario may already support another space
        supports = [
            #support_spec{provider = Provider, storage_spec = #posix_storage_params{
                mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>
            }} || Provider <- [krakow, paris]
        ]
    }),
    SpaceDirGuid = space_dir:guid(SpaceId),
    ?assertMatch({ok, _}, rpc:call(ParisNode, lfm, mkdir,
        [SessId, SpaceDirGuid, generator:gen_name(), 8#755]), ?ATTEMPTS),

    {ok, StorageId} = rpc:call(ParisNode, space_logic, get_local_supporting_storage, [SpaceId]),
    ?assertEqual(ok, rpc:call(ParisNode, storage_logic, revoke_space_support, [StorageId, SpaceId])),
    ?assertEqual(false, rpc:call(ParisNode, provider_logic, supports_space, [SpaceId]), ?ATTEMPTS),

    Name = generator:gen_name(),
    ?assertEqual({error, ?EINVAL}, rpc:call(ParisNode, lfm, ensure_dir,
        [SessId, SpaceDirGuid, Name, 8#755])),
    ?assertEqual({error, ?EINVAL}, rpc:call(ParisNode, lfm, resolve_guid_by_relative_path,
        [SessId, SpaceDirGuid, Name])),
    ?assertEqual({error, not_found}, rpc:call(ParisNode, file_meta_forest, get,
        [file_id:guid_to_uuid(SpaceDirGuid), all, Name])).


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
-spec get_children_attrs(ssl:sslsocket(), file_id:file_guid()) ->
    {ok, #'FileChildrenAttrs'{}} | {error, atom()}.
get_children_attrs(Sock, Guid) ->
    send_fuse_request(Sock, {file_request, #'FileRequest'{
        context_guid = Guid,
        file_request = {get_file_children_attrs, #'GetFileChildrenAttrs'{offset = 0, size = 100}}
    }}).


%% @private
-spec resolve_guid(ssl:sslsocket(), file_meta:path()) -> {ok, #'Uuid'{}} | {error, atom()}.
resolve_guid(Sock, Path) ->
    send_fuse_request(Sock, {resolve_guid, #'ResolveGuid'{path = Path}}).


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
