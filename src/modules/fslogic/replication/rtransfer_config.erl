%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Rtransfer config and start.
%%% @end
%%%--------------------------------------------------------------------
-module(rtransfer_config).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/rtransfer_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-define(RTRANSFER_PORT, proplists:get_value(server_port,
                                            application:get_env(rtransfer_link, transfer, []),
                                            6665)).

-define(MOCK, application:get_env(?APP_NAME, rtransfer_mock, false)).

%% API
-export([start_rtransfer/0, restart_link/0, fetch/6]).
-export([get_nodes/1, open/2, fsync/1, close/1, auth_request/2, get_connection_secret/2]).
-export([add_storage/1, generate_secret/2]).
-export([get_local_ip_and_port/0]).

%% Dialyzer doesn't find the behaviour
%-behaviour(rtransfer_link_callback).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Start rtransfer server
%% @end
%%--------------------------------------------------------------------
-spec start_rtransfer() -> {ok, pid()}.
start_rtransfer() ->
    prepare_ssl_opts(),
    prepare_graphite_opts(),
    {ok, RtransferPid} = rtransfer_link_sup:start_link(no_cluster),
    rtransfer_link:set_provider_nodes([node()], ?MODULE),
    StorageDocs = get_storages(10),
    lists:foreach(fun add_storage/1, StorageDocs),
    {ok, RtransferPid}.

%%--------------------------------------------------------------------
%% @doc
%% Restarts only `link` native application, forcing a reload of
%% certificates and state. Ongoing tasks will be briefly interrupted
%% but then resumed.
%% @end
%%--------------------------------------------------------------------
restart_link() ->
    case whereis(rtransfer_link_port) of
        undefined -> {error, not_running};
        Pid ->
            prepare_ssl_opts(),
            prepare_graphite_opts(),
            erlang:exit(Pid, restarting),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Realizes a given fetch request.
%% NotifyFun is called on every request update. CompleteFun is called
%% with result of the fetch.
%% @end
%%--------------------------------------------------------------------
-spec fetch(rtransfer_link_request:t(), rtransfer_link:notify_fun(),
            rtransfer_link:on_complete_fun(), TransferId :: binary(),
            SpaceId :: binary(), FileGuid :: binary()) ->
                   {ok, reference()} | {error, Reason :: any()}.
fetch(Request, NotifyFun, CompleteFun, TransferId, SpaceId, FileGuid) ->
    case ?MOCK of
        true ->
            #{offset := O, size := S} = Request,
            Ref = make_ref(),
            NotifyFun(Ref, O, S),
            CompleteFun(Ref, {ok, ok}),
            {ok, Ref};
        _ ->
            TransferData = erlang:term_to_binary({TransferId, SpaceId, FileGuid}),
            fetch(Request, TransferData, NotifyFun, CompleteFun, 3)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of node addresses for a given provider.
%% @end
%%--------------------------------------------------------------------
-spec get_nodes(ProviderId :: binary()) -> rtransfer_link:address().
get_nodes(ProviderId) ->
    {ok, IPs} = provider_logic:resolve_ips(ProviderId),
    [{IP, ?RTRANSFER_PORT} || IP <- IPs].

%%--------------------------------------------------------------------
%% @doc
%% Opens a file.
%% @end
%%--------------------------------------------------------------------
-spec open(FileUUID :: binary(), read | write) ->
    {ok, Handle :: term()} | {error, Reason :: any()}.
open(FileGUID, _OpenFlag) ->
    % TODO vfs-4412 - delete second arg and change name
    sfm_utils:create_delayed_storage_file(file_ctx:new_by_guid(FileGUID)),
    {ok, undefined}.

%%--------------------------------------------------------------------
%% @doc
%% Calls fsync on a file handle opened with {@link open/2}.
%% @end
%%--------------------------------------------------------------------
-spec fsync(Handle :: term()) -> any().
fsync(_Handle) ->
    % TODO vfs-4412 - delete callback
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Releases a file handle opened with {@link open/2}.
%% @end
%%--------------------------------------------------------------------
-spec close(Handle :: term()) -> any().
close(_Handle) ->
    % TODO vfs-4412 - delete callback
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Authorize a transfer of a specific file.
%% @end
%%--------------------------------------------------------------------
-spec auth_request(TransferData :: binary(), ProviderId :: binary()) ->
                          false | {storage:id(), helpers:file_id(), fslogic_worker:file_guid()}.
auth_request(TransferData, ProviderId) ->
    try
        %% TransferId is not verified because provider could've created the transfer
        %% if it wanted to. Plus, the transfer will most often start before the
        %% transfer document is created.
        {_TransferId, SpaceId, FileGuid} = erlang:binary_to_term(TransferData, [safe]),

        SpaceDoc =
            case space_logic:get(?ROOT_SESS_ID, SpaceId) of
                {ok, SD} -> SD;
                {error, Reason} -> throw({error, {cannot_get_space_document, SpaceId, Reason}})
            end,

        case space_logic:is_supported(SpaceDoc, ProviderId) of
            true -> ok;
            false -> throw({error, space_not_supported_by_remote_provider, SpaceId})
        end,

        case space_logic:is_supported(SpaceDoc, oneprovider:get_id_or_undefined()) of
            true -> ok;
            false -> throw({error, space_not_supported_by_local_provider, SpaceId})
        end,

        case fslogic_uuid:guid_to_space_id(FileGuid) of
            SpaceId -> ok;
            _ -> throw({error, {invalid_file_guid, FileGuid}})
        end,

        FileCtx = file_ctx:new_by_guid(FileGuid),
        {Loc, _} = file_ctx:get_local_file_location_doc(FileCtx, false),
        #document{value =
                      #file_location{storage_id = StorageId,
                                     file_id = FileId}} = Loc,

        {StorageId, FileId, FileGuid}
    catch
        _:Err ->
            ?error_stacktrace("Auth providerid=~p failed due to ~p", [ProviderId, Err]),
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Asks a remote provider for its connection secret needed to
%% authenticate. Provides the remote with our own secret so that the
%% authentication can be mutual.
%% @end
%%--------------------------------------------------------------------
-spec get_connection_secret(ProviderId :: binary(),
                            rtransfer_link:address()) ->
                                   {MySecret :: binary(), PeerSecret :: binary()}.
get_connection_secret(ProviderId, {_Host, _Port}) ->
    SessId = session_utils:get_provider_session_id(outgoing, ProviderId),
    MySecret = do_generate_secret(),
    Request = #generate_rtransfer_conn_secret{secret = MySecret},
    {ok, #server_message{message_body = #rtransfer_conn_secret{secret = PeerSecret}}} =
        communicator:communicate_with_provider(SessId, Request),
    {MySecret, PeerSecret}.

%%--------------------------------------------------------------------
%% @doc
%% Adds storage to rtransfer.
%% @end
%%--------------------------------------------------------------------
-spec add_storage(storage:doc()) -> any().
add_storage(#document{key = StorageId, value = #storage{}} = Storage) ->
    Helper = hd(storage:get_helpers(Storage)),
    HelperParams = helper:get_params(Helper, helper:get_admin_ctx(Helper)),
    HelperName = helper:get_name(HelperParams),
    HelperArgs = maps:to_list(helper:get_args(HelperParams)),
    {_, BadNodes} = rpc:multicall(consistent_hasing:get_all_nodes(),
                                  rtransfer_link, add_storage,
                                  [StorageId, HelperName, HelperArgs]),
    BadNodes =/= [] andalso
        ?error("Failed to add storage ~p on nodes ~p", [StorageId, BadNodes]).

%%--------------------------------------------------------------------
%% @doc
%% Generates this provider's connection secret needed for a client
%% rtransfer to establish new connections to this rtransfer.
%% @end
%%--------------------------------------------------------------------
-spec generate_secret(ProviderId :: binary(), PeerSecret :: binary()) -> binary().
generate_secret(ProviderId, PeerSecret) ->
    MySecret = do_generate_secret(),
    {_, BadNodes} = rpc:multicall(consistent_hasing:get_all_nodes(),
                                  rtransfer_link, allow_connection,
                                  [ProviderId, MySecret, PeerSecret, 60000]),
    BadNodes =/= [] andalso
        ?error("Failed to allow rtransfer connection from ~p on nodes ~p",
               [ProviderId, BadNodes]),
    MySecret.

%%--------------------------------------------------------------------
%% @doc
%% Generates this provider's connection secret needed for a client
%% rtransfer to establish new connections to this rtransfer.
%% @end
%%--------------------------------------------------------------------
-spec get_local_ip_and_port() -> {IP :: inet:ip4_address(), Port :: 0..65535}.
get_local_ip_and_port() ->
    IP = node_manager:get_ip_address(),
    Port = ?RTRANSFER_PORT,
    {IP, Port}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates a 64-byte random secret.
%% @end
%%--------------------------------------------------------------------
-spec do_generate_secret() -> binary().
do_generate_secret() ->
    RealSecret = crypto:strong_rand_bytes(32),
    PaddingSize = (64 - byte_size(RealSecret)) * 8,
    <<RealSecret/binary, 0:PaddingSize>>.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets application environment for rtransfer_link to use SSL with
%% provider's certs.
%% @end
%%--------------------------------------------------------------------
-spec prepare_ssl_opts() -> any().
prepare_ssl_opts() ->
    OriginalSSLOpts = application:get_env(rtransfer_link, ssl, []),
    case proplists:get_value(use_ssl, OriginalSSLOpts, true) of
        true ->
            {ok, KeyFile} = application:get_env(?APP_NAME, web_key_file),
            CABundle = make_ca_bundle(),
            CertBundle = make_cert_bundle(),
            Opts = [{use_ssl, true}, {cert_path, CertBundle}, {key_path, KeyFile} |
                    [{ca_path, CABundle} || CABundle /= false]],
            application:set_env(rtransfer_link, ssl, Opts, [{persistent, true}]);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a bundle file from all CA certificates in the cacerts dir.
%% @end
%%--------------------------------------------------------------------
-spec make_ca_bundle() -> file:filename() | false.
make_ca_bundle() ->
    CADir = oz_plugin:get_cacerts_dir(),
    {ok, CertPems} = file_utils:read_files({dir, CADir}),
    write_certs_to_temp(CertPems).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a bundle file from this providers' certificate and chain.
%% @end
%%--------------------------------------------------------------------
-spec make_cert_bundle() -> file:filename().
make_cert_bundle() ->
    {ok, CertFile} = application:get_env(?APP_NAME, web_cert_file),
    {ok, ChainFile} = application:get_env(?APP_NAME, web_cert_chain_file),
    {ok, Cert} = file:read_file(CertFile),
    Contents =
        case file:read_file(ChainFile) of
            {ok, Chain} -> [Cert, Chain];
            Error ->
                ?warning("Error reading certificate chain in path ~p: ~p. "
                         "rtransfer will use cert only", [ChainFile, Error]),
                [Cert]
        end,
    Filename = write_certs_to_temp(Contents),
    true = is_list(Filename),
    Filename.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a temporary file populated with given contents.
%% @end
%%--------------------------------------------------------------------
-spec write_certs_to_temp(Contents :: [binary()]) -> file:filename() | false.
write_certs_to_temp(Contents) ->
    case lists:flatmap(fun public_key:pem_decode/1, Contents) of
        [] -> false;
        Ders ->
            TempPath = lib:nonl(os:cmd("mktemp")),
            OutData = public_key:pem_encode(Ders),
            ok = file:write_file(TempPath, OutData),
            TempPath
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets application environment for rtransfer_link to use graphite
%% connection with provider's settings.
%% @end
%%--------------------------------------------------------------------
-spec prepare_graphite_opts() -> any().
prepare_graphite_opts() ->
    case application:get_env(?APP_NAME, integrate_with_graphite, false) of
        false -> ok;
        true ->
            case application:get_env(?APP_NAME, graphite_api_key) of
                {ok, Bin} when byte_size(Bin) > 0 ->
                    ?error("rtransfer_link doesn't support graphite access with API key", []);
                _ ->
                    {ok, Host} = application:get_env(?APP_NAME, graphite_host),
                    {ok, Port} = application:get_env(?APP_NAME, graphite_port),
                    {ok, Prefix} = application:get_env(?APP_NAME, graphite_prefix),
                    NewPrefix = unicode:characters_to_list(Prefix) ++ "-rtransfer.link",
                    Url = "http://" ++ unicode:characters_to_list(Host) ++ ":" ++
                        integer_to_list(Port),
                    Opts = [{graphite_url, Url}, {graphite_namespace_prefix, NewPrefix}],
                    application:set_env(rtransfer_link, monitoring, Opts)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls fetch on rtransfer_link.
%% @end
%%--------------------------------------------------------------------
-spec fetch(rtransfer_link_request:t(), binary(), rtransfer_link:notify_fun(),
    rtransfer_link:on_complete_fun(), non_neg_integer()) ->
    {ok, reference()} | {error, Reason :: any()}.
fetch(_Request, _TransferData, _NotifyFun, _CompleteFun, 0) ->
    {error, rtransfer_link_internal_error};
fetch(Request, TransferData, NotifyFun, CompleteFun, RetryNum) ->
    try
        rtransfer_link:fetch(Request, TransferData, NotifyFun, CompleteFun)
    catch
        %% The process we called was already terminating because of idle timeout,
        %% there's nothing to worry about.
        exit:{{shutdown, timeout}, _} ->
            ?warning("Rtransfer fetch failed because of a timeout, "
            "retrying with a new one"),
            fetch(Request, TransferData, NotifyFun, CompleteFun, RetryNum - 1);
        _:{noproc, _} ->
            ?warning("Rtransfer fetch failed because of noproc, "
            "retrying with a new one"),
            fetch(Request, TransferData, NotifyFun, CompleteFun, RetryNum - 1);
        exit:{normal, _} ->
            ?warning("Rtransfer fetch failed because of exit:normal, "
            "retrying with a new one"),
            fetch(Request, TransferData, NotifyFun, CompleteFun, RetryNum - 1)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get storages list. Retry if needed.
%% @end
%%--------------------------------------------------------------------
-spec get_storages(non_neg_integer()) -> [storage:doc()] | {error, term()}.
get_storages(Num) ->
    case {storage:list(), Num} of
        {{ok, StorageDocs}, _} ->
            StorageDocs;
        {Error, 0} ->
            Error;
        _ ->
            timer:sleep(500),
            get_storages(Num - 1)
    end.