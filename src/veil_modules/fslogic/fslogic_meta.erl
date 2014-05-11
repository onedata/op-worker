%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Write me !
%% @end
%% ===================================================================
-module(fslogic_meta).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([update_parent_ctime/2, update_meta_attr/3]).

%% ====================================================================
%% API functions
%% ====================================================================


%% update_meta_attr/3
%% ====================================================================
%% @doc Updates file_meta record associated with given #file record. <br/>
%%      Attr agument decides which field has to be updated with Value. <br/>
%%      There is one exception to this rule: if Attr == 'times', Value has to be tuple <br/>
%%      with fallowing format: {ATimeValue, MTimeValue, CTimeValue} or {ATimeValue, MTimeValue}. <br/>
%%      If there is no #file_meta record associated with given #file, #file_meta will be created and whole function call will be blocking. <br/>
%%      Otherwise the method call will be asynchronous. <br/> Returns given as argument #file record unchanged, unless #file_meta had to be created. <br/>
%%      In this case returned #file record will have #file.meta_doc field updated and shall be saved to DB after this call.
%% @end
-spec update_meta_attr(File :: #file{}, Attr, Value :: term()) -> Result :: #file{} when
    Attr :: atime | mtime | ctime | size | times.
%% ====================================================================
update_meta_attr(File, Attr, Value) ->
    update_meta_attr(File, Attr, Value, 5).

%% Updates modification time for parent of Dir
update_parent_ctime(Dir, CTime) ->
    case fslogic_utils:strip_path_leaf(Dir) of
        [?PATH_SEPARATOR] -> ok;
        ParentPath ->
            try
                gen_server:call(?Dispatcher_Name, {fslogic, 1, #veil_request{subject = get(user_id), request = {internal_call, #updatetimes{file_logic_name = ParentPath, mtime = CTime}}}})
            catch
                E1:E2 ->
                    lager:error("update_parent_ctime error: ~p:~p", [E1, E2]),
                    error
            end
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% update_meta_attr/4
%% ====================================================================
%% @doc Internal implementation of update_meta_attr/3. See update_meta_attr/3 for more information.
%% @end
-spec update_meta_attr(File :: #file{}, Attr, Value :: term(), RetryCount :: integer()) -> Result :: #file{} when
    Attr :: atime | mtime | ctime | size | times.
update_meta_attr(#file{meta_doc = MetaUUID} = File, Attr, Value, RetryCount) ->
    SyncTask = fun() ->
        case init_file_meta(File) of
            {File1, #veil_document{record = MetaRec} = MetaDoc} ->
                NewMeta =
                    case Attr of
                        times ->
                            case Value of
                                {ATime, MTime, CTime}                    -> MetaRec#file_meta{uid = File#file.uid, atime = ATime, mtime = MTime, ctime = CTime};
                                {ATime, MTime} when ATime > 0, MTime > 0 -> MetaRec#file_meta{uid = File#file.uid, atime = ATime, mtime = MTime};
                                {ATime, _MTime} when ATime > 0           -> MetaRec#file_meta{uid = File#file.uid, atime = ATime};
                                {_ATime, MTime} when MTime > 0           -> MetaRec#file_meta{uid = File#file.uid, mtime = MTime}
                            end;
                        ctime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, ctime = Value};
                        mtime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, mtime = Value};
                        atime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, atime = Value};
                        size when Value >= 0 ->
                            MetaRec#file_meta{uid = File#file.uid, size = Value};
                        _ ->
                            MetaRec
                    end,
                case MetaRec of
                    NewMeta -> File1;
                    _ ->
                        NewDoc = MetaDoc#veil_document{record = NewMeta},
                        case dao_lib:apply(dao_vfs, save_file_meta, [NewDoc], 1) of
                            {ok, _} -> File1;
                            {error, conflict} when RetryCount > 0 ->
                                lager:warning("Conflict when saveing file_meta record for file (name = ~p, parent = ~p). Retring...", [File#file.name, File#file.parent]),
                                {_, _, M} = now(),
                                timer:sleep(M rem 100), %% If case of conflict we should wait a little bit before next try (max 100ms)
                                update_meta_attr(File1, Attr, Value, RetryCount - 1);
                            {error, Error} ->
                                lager:warning("Cannot save file_meta record for file (name = ~p, parent = ~p) due to error: ~p", [File#file.name, File#file.parent, Error])
                        end
                end;
            _Error ->
                lager:warning("Cannot init file_meta record for file (name = ~p, parent = ~p) due to previous errors", [File#file.name, File#file.parent]),
                File
        end
    end, %% SyncTask = fun()

    case MetaUUID of
        UUID when is_list(UUID) -> %% When MetaUUID is set, run this method async
            spawn(SyncTask),
            File;
        _ ->
            SyncTask()
    end.


%% init_file_meta/1
%% ====================================================================
%% @doc Internal implementation of update_meta_attr/3. This method handles creation of not existing #file_meta document.
%% @end
-spec init_file_meta(File :: #file{}) -> Result :: {#file{}, term()}.
%% ====================================================================
init_file_meta(#file{meta_doc = MetaUUID} = File) when is_list(MetaUUID) ->
    case dao_lib:apply(dao_vfs, get_file_meta, [MetaUUID], 1)  of
        {ok, #veil_document{} = MetaDoc} -> {File, MetaDoc};
        Error ->
            lager:error("File (name = ~p, parent = ~p) points to file_meta (uuid = ~p) that is not available. DAO response: ~p", [File#file.name, File#file.parent, MetaUUID, Error]),
            {File, #veil_document{uuid = MetaUUID, record = #file_meta{}}}
    end;
init_file_meta(#file{} = File) ->
    case dao_lib:apply(dao_vfs, save_file_meta, [#file_meta{uid = File#file.uid}], 1) of
        {ok, MetaUUID} when is_list(MetaUUID) -> init_file_meta(File#file{meta_doc = MetaUUID});
        Error ->
            lager:error("Cannot save file_meta record for file (name = ~p, parent = ~p) due to: ~p", [File#file.name, File#file.parent, Error]),
            {File, undefined}
    end.
