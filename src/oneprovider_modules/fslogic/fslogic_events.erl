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
-include("oneprovider_modules/dao/dao_types.hrl").
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

%% on_file_size_update/3
%% ====================================================================
%% @doc Shall be called whenever size of the file has changed.
-spec on_file_size_update(FileUUID :: uuid(), OldFileSize :: non_neg_integer(), NewFileSize :: non_neg_integer()) -> ok.
%% ====================================================================
on_file_size_update(FileUUID, OldFileSize, NewFileSize) ->
    gen_server:call(request_dispatcher, {fslogic, 1, {internal_event, on_file_size_update, {FileUUID, OldFileSize, NewFileSize}}}, timer:seconds(5)).


%% on_file_meta_update/2
%% ====================================================================
%% @doc Shall be called whenever file_meta document of the file has changed.
-spec on_file_meta_update(FileUUID :: uuid(), Doc :: db_doc()) -> ok.
%% ====================================================================
on_file_meta_update(FileUUID, Doc) ->
    gen_server:call(request_dispatcher, {fslogic, 1, {internal_event, on_file_meta_update, {FileUUID, Doc}}}, timer:seconds(5)).

%% ===================================================================
%% Handlers
%% ===================================================================

%% handle_event/2
%% ====================================================================
%% @doc Handles events in fslogic worker context
-spec handle_event(EventType :: atom(), Args :: term()) -> ok.
%% ====================================================================
handle_event(on_file_size_update, {FileUUID, _OldFileSize, _NewFileSize}) ->
    delayed_push_attrs(FileUUID);
handle_event(on_file_meta_update, {FileUUID, _Doc}) ->
    delayed_push_attrs(FileUUID);
handle_event(EventType, _Args) ->
    ?warning("Unknown event with type: ~p", [EventType]),
    ok.


%% delayed_push_attrs/1
%% ====================================================================
%% @doc Mark the file's attributes to be updated in fuse clients within next second.
-spec delayed_push_attrs(FileUUID :: uuid()) -> ok.
%% ====================================================================
delayed_push_attrs(FileUUID) ->
    case ets:lookup(?fslogic_attr_events_state, utils:ensure_binary(FileUUID)) of
        [{_, _TRef}] -> ok;
        [] ->
            {ok, TRef} = timer:apply_after(timer:seconds(1), fslogic_events, push_new_attrs, [FileUUID]),
            ets:insert(?fslogic_attr_events_state, {utils:ensure_binary(FileUUID), TRef}),
            ok
    end.


%% push_new_attrs/1
%% ====================================================================
%% @doc Pushes current file's attributes to all fuses that are currently using this file
-spec push_new_attrs(FileUUID :: uuid()) -> [Result :: ok | {error, Reason :: any()}].
%% ====================================================================
push_new_attrs(FileUUID) ->
    Res0 = push_new_attrs3(list_descriptors, fun(#file_descriptor{fuse_id = FID}) -> FID end,
                            FileUUID, 0, 100),
    Res1 = push_new_attrs3(list_attr_watchers, fun(#file_attr_watcher{fuse_id = FID}) -> FID end,
                            FileUUID, 0, 100),
    lists:flatten([Res0, Res1]).
push_new_attrs3(ListMethod, RecToFuseId, FileUUID, Offset, Count) ->
    ets:delete(?fslogic_attr_events_state, utils:ensure_binary(FileUUID)),
    {ok, FDs} = dao_lib:apply(dao_vfs, ListMethod, [{by_uuid_n_owner, {utils:ensure_list(FileUUID), ""}}, Count, Offset], 1),
    Fuses0 = lists:map(
        fun(#db_document{record = Record}) ->
            RecToFuseId(Record)
        end, FDs),
    Fuses1 = lists:usort(Fuses0),
    ?info("Pushing new attributes for file ~p to fuses ~p", [FileUUID, Fuses1]),

    case Fuses1 of
        [] -> [];
        FuseIDs ->
            {ok, #db_document{} = FileDoc} = fslogic_objects:get_file({uuid, FileUUID}),
            Attrs = #fileattr{} = fslogic_req_generic:get_file_attr(FileDoc),

            Results = lists:map(
                fun(FuseID) ->
                    Res = request_dispatcher:send_to_fuse(FuseID, Attrs, "fuse_messages"),
                    ?debug("Sending msg to fuse ~p: ~p", [FuseID, Res]),
                    Res
                end, FuseIDs),
            [push_new_attrs3(ListMethod, RecToFuseId, FileUUID, Offset + Count, 100) | Results]
    end.