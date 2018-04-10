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
-include("modules/events/definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/rtransfer_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-define(RTRANSFER_PORT, proplists:get_value(server_port,
                                            application:get_env(rtransfer_link, transfer, []),
                                            6665)).

%% API
-export([start_rtransfer/0, fetch/6]).
-export([get_nodes/1, open/2, fsync/1, close/1, auth_request/2, get_connection_secret/2]).
-export([add_storage/1, generate_secret/2]).

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
    {ok, StorageDocs} = storage:list(),
    lists:foreach(fun add_storage/1, StorageDocs),
    {ok, RtransferPid}.

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
    TransferData = erlang:term_to_binary({TransferId, SpaceId, FileGuid}),
    rtransfer_link:fetch(Request, TransferData, NotifyFun, CompleteFun).

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
open(FileGUID, OpenFlag) ->
    lfm_files:open(?ROOT_SESS_ID, {guid, FileGUID}, OpenFlag).

%%--------------------------------------------------------------------
%% @doc
%% Calls fsync on a file handle opened with {@link open/2}.
%% @end
%%--------------------------------------------------------------------
-spec fsync(Handle :: term()) -> any().
fsync(Handle) ->
    lfm_files:fsync(Handle).

%%--------------------------------------------------------------------
%% @doc
%% Releases a file handle opened with {@link open/2}.
%% @end
%%--------------------------------------------------------------------
-spec close(Handle :: term()) -> any().
close(Handle) ->
    lfm_files:release(Handle).

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
        {Loc, _} = file_ctx:get_local_file_location_doc(FileCtx),
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
    SessId = session_manager:get_provider_session_id(outgoing, ProviderId),
    MySecret = do_generate_secret(),
    Request = #generate_rtransfer_conn_secret{secret = MySecret},
    {ok, #server_message{message_body = #rtransfer_conn_secret{secret = PeerSecret}}} =
        provider_communicator:communicate(Request, SessId),
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
