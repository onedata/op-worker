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
-include_lib("ctool/include/logging.hrl").

%% API
-export([on_file_size_update/3, on_file_meta_update/2]).

on_file_size_update(FileUUID, OldFileSize, NewFileSize) ->
    push_new_attrs(FileUUID).

on_file_meta_update(FileUUID, Doc) ->
    push_new_attrs(FileUUID).


push_new_attrs(FileUUID) ->
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