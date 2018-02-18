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
-export([get_nodes/1, open/2, fsync/1, close/1, auth_request/4, get_connection_secret/2]).
-export([add_storage/1, generate_secret/2]).

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
    {ok, RtransferPid} = rtransfer_link_sup:start_link(no_cluster),
    rtransfer_link:set_provider_nodes([node()], ?MODULE),
    {ok, StorageDocs} = storage:list(),
    lists:foreach(fun add_storage/1, StorageDocs),

    SubId = uuid:uuid_to_string(uuid:get_v4(), binary_standard),
    EventHandler =
        fun(Events, _) ->
                #quota_exceeded_event{spaces = Spaces} = lists:last(Events),
                rtransfer_link_quota_manager:update_disabled_spaces(Spaces)
        end,
    event:subscribe(#subscription{
                       id = SubId,
                       type = #quota_exceeded_subscription{},
                       stream = #event_stream{event_handler = EventHandler}
                      }, ?ROOT_SESS_ID),

    {ok, RtransferPid}.

fetch(Request, NotifyFun, CompleteFun, TransferId, SpaceId, FileGuid) ->
    TransferData = erlang:term_to_binary({TransferId, SpaceId, FileGuid}),
    rtransfer_link:fetch(Request, TransferData, NotifyFun, CompleteFun).

-spec get_nodes(ProviderId :: binary()) -> rtransfer_link:address().
get_nodes(ProviderId) ->
    {ok, IPs} = provider_logic:resolve_ips(ProviderId),
    [{IP, ?RTRANSFER_PORT} || IP <- IPs].

-spec open(FileUUID :: binary(), read | write) ->
    {ok, Handle :: term()} | {error, Reason :: any()}.
open(FileGUID, OpenFlag) ->
    lfm_files:open(?ROOT_SESS_ID, {guid, FileGUID}, OpenFlag).

-spec fsync(Handle :: term()) -> any().
fsync(Handle) ->
    lfm_files:fsync(Handle).

-spec close(Handle :: term()) -> any().
close(Handle) ->
    lfm_files:release(Handle).

-spec auth_request(TransferData :: binary(), StorageId :: binary(),
                   FileId :: binary(), ProviderId :: binary()) -> boolean().
auth_request(TransferData, StorageId, FileId, ProviderId) ->
    try
        {TransferId, SpaceId, FileGuid} = erlang:binary_to_term(TransferData, [safe]),
        %% Transfer = case transfer:get(TransferId) of
        %%                {ok, #document{value = T}} -> T;
        %%                _ -> throw({error, invalid_transfer_id})
        %%            end,

        %% case Transfer of
        %%     #transfer{space_id = SpaceId} -> ok;
        %%     _ -> throw({error, invalid_space_id})
        %% end,

        SpaceDoc =
            case space_logic:get(?ROOT_SESS_ID, SpaceId) of
                {ok, SD} -> SD;
                {error, Reason} -> throw({error, {cannot_get_space_document, Reason}})
            end,

        case space_logic:is_supported(SpaceDoc, ProviderId) of
            true -> ok;
            false -> throw({error, space_not_supported_by_remote_provider})
        end,

        case space_logic:is_supported(SpaceDoc, oneprovider:get_id_or_undefined()) of
            true -> ok;
            false -> throw({error, space_not_supported_by_local_provider})
        end,

        case fslogic_uuid:guid_to_space_id(FileGuid) of
            SpaceId -> ok;
            _ -> throw({error, invalid_file_guid})
        end,

        FileCtx = file_ctx:new_by_guid(FileGuid),
        {Loc, _} = file_ctx:get_local_file_location_doc(FileCtx),
        case Loc of
            #document{value = #file_location{
                                 storage_id = StorageId,
                                 file_id = FileId}} -> ok;
            _ -> throw({error, invalid_file_id_or_storage_id})
        end,

        true
    catch
        _:Err ->
            ?error_stacktrace("Auth of storageid=~p, fileid=~p, providerid=~p failed due to ~p",
                              [StorageId, FileId, ProviderId, Err]),
            false
    end.

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

-spec add_storage(storage:doc()) -> any().
add_storage(#document{key = StorageId, value = #storage{}} = Storage) ->
    Helper = hd(storage:get_helpers(Storage)),
    HelperParams = helper:get_params(Helper, helper:get_admin_ctx(Helper)),
    HelperName = helper:get_name(HelperParams),
    HelperArgs = maps:to_list(helper:get_args(HelperParams)),
    rtransfer_link:add_storage(StorageId, HelperName, HelperArgs).

-spec generate_secret(ProviderId :: binary(), PeerSecret :: binary()) -> binary().
generate_secret(ProviderId, PeerSecret) ->
    MySecret = do_generate_secret(),
    rtransfer_link:allow_connection(ProviderId, MySecret, PeerSecret, 60000),
    MySecret.

-spec do_generate_secret() -> binary().
do_generate_secret() ->
    RealSecret = crypto:strong_rand_bytes(32),
    PaddingSize = (64 - byte_size(RealSecret)) * 8,
    <<RealSecret/binary, 0:PaddingSize>>.

%%%===================================================================
%%% Internal functions
%%%===================================================================
