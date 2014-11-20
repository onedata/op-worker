%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: fslogic events handlers and callbacks
%% @end
%% ===================================================================
-module(fslogic_events).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([on_file_size_update/3, on_file_meta_update/2]).
-export([handle_event/2]).
-export([push_new_attrs/1]).

%% ===================================================================
%% Triggers
%% ===================================================================

on_file_size_update(FileUUID, OldFileSize, NewFileSize) ->
    gen_server:call(request_dispatcher, {fslogic, 1, {internal_event, on_file_size_update, {FileUUID, OldFileSize, NewFileSize}}}).

on_file_meta_update(FileUUID, Doc) ->
    gen_server:call(request_dispatcher, {fslogic, 1, {internal_event, on_file_meta_update, {FileUUID, Doc}}}).

%% ===================================================================
%% Handlers
%% ===================================================================
handle_event(on_file_size_update, {FileUUID, _OldFileSize, _NewFileSize}) ->
    delayed_push_attrs(FileUUID);
handle_event(on_file_meta_update, {FileUUID, _Doc}) ->
    delayed_push_attrs(FileUUID);
handle_event(EventType, _Args) ->
    ?warning("Unknown event with type: ~p", [EventType]).


delayed_push_attrs(FileUUID) ->
    case ets:lookup(?fslogic_attr_events_state, utils:ensure_binary(FileUUID)) of
        [{_, _TRef}] -> ok;
        [] ->
            {ok, TRef} = timer:apply_after(timer:seconds(1), fslogic_events, push_new_attrs, [FileUUID]),
            ets:insert(?fslogic_attr_events_state, {utils:ensure_binary(FileUUID), TRef}),
            ok
    end.


push_new_attrs(FileUUID) ->
    ets:delete(?fslogic_attr_events_state, utils:ensure_binary(FileUUID)),
    {ok, FDs} = dao_lib:apply(dao_vfs, list_descriptors, [{by_uuid_n_owner, {FileUUID, ""}}, 100000, 0], 1),
    Fuses0 = lists:map(
        fun(#db_document{record = #file_descriptor{fuse_id = FuseID}}) ->
            FuseID
        end, FDs),
    Fuses1 = lists:usort(Fuses0),
    ?info("Pushing new attributes for file ~p to fuses ~p", [FileUUID, Fuses1]),

    case Fuses1 of
        [] -> ok;
        FuseIDs ->
            {ok, #db_document{} = FileDoc} = fslogic_objects:get_file({uuid, FileUUID}),
            Attrs = #fileattr{} = fslogic_req_generic:get_file_attr(FileDoc),

            lists:foreach(
                fun(FuseID) ->
                    Res = request_dispatcher:send_to_fuse(FuseID, Attrs, "fuse_messages"),
                    ?info("Sending msg to fuse ~p: ~p", [FuseID, Res])
                end, FuseIDs)
    end.