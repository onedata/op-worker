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

-include("modules/fslogic/lfm_internal.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

-define(RTRANSFER_PORT, 6665).
-define(RTRANSFER_NUM_ACCEPTORS, 10).

%% API
-export([start_rtransfer/0]).

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
    Opts = [
        {get_nodes_fun,
            fun(ProviderId) ->
                {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
                lists:map(
                    fun(URL) ->
                        {ok, Ip} = inet:ip(binary_to_list(URL)),
                        {Ip, ?RTRANSFER_PORT}
                    end, URLs)
            end},
        {open_fun,
            fun(FileUUID, OpenMode) ->
                CTX = fslogic_context:new(?ROOT_SESS_ID),
                {ok, FileDoc} = file_meta:get({uuid, FileUUID}),
                {ok, #document{key = StorageId, value = Storage}} = fslogic_storage:select_storage(CTX#fslogic_ctx.space_id),
                FileId = fslogic_utils:gen_storage_file_id({uuid, FileUUID}),
                {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),

                SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUUID, FileUUID, Storage, FileId),
                case storage_file_manager:open(SFMHandle0, OpenMode) of
                    {ok, NewSFMHandle} ->
                        {ok, #lfm_handle{sfm_handles = maps:from_list([{default, {{StorageId, FileId}, NewSFMHandle}}]),
                            fslogic_ctx = CTX, file_uuid = FileUUID, open_mode = OpenMode}};
                    {error, Reason} ->
                        {error, Reason}
                end
            end},
        {read_fun,
            fun(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_mode = OpenType} = Handle, Offset, MaxSize) ->
                {Key, NewSize} = lfm_files:get_sfm_handle_key(UUID, Offset, MaxSize),
                {{StorageId, FileId}, SFMHandle, NewHandle} = lfm_files:get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

                case storage_file_manager:read(SFMHandle, Offset, NewSize) of
                    {ok, Data} ->
                        {ok, NewHandle, Data};
                    {error, Reason} ->
                        {error, Reason}
                end
            end},
        {write_fun,
            fun(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_mode = OpenType} = Handle, Offset, Buffer) ->
                {Key, NewSize} = lfm_files:get_sfm_handle_key(UUID, Offset, byte_size(Buffer)),
                {{StorageId, FileId}, SFMHandle, NewHandle} = lfm_files:get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

                case storage_file_manager:write(SFMHandle, Offset, binary:part(Buffer, 0, NewSize)) of
                    {ok, Written} ->
                        {ok, NewHandle, Written};
                    {error, Reason2} ->
                        {error, Reason2}
                end
            end},
        {close_fun,
            fun(_Handle) ->
                ok
            end},
        {ranch_opts,
            [
                {num_acceptors, ?RTRANSFER_NUM_ACCEPTORS},
                {transport, ranch_tcp},
                {trans_opts, [{port, ?RTRANSFER_PORT}]}
            ]
        }
    ],
    {ok, _} = rtransfer:start_link(Opts).

%%%===================================================================
%%% Internal functions
%%%===================================================================