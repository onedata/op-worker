%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides mathods for #file_meta record manipulation
%%       such as e.g. access times update.
%% @end
%% ===================================================================
-module(fslogic_meta).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update_parent_ctime/2, update_meta_attr/3, update_meta_attr/4, update_user_files_size_view/1]).

-ifdef(TEST).
-export([init_file_meta/1]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% update_user_files_size_view
%% ====================================================================
%% @doc Updates user files size view in db
%% @end
-spec update_user_files_size_view(ProtocolVersion :: term()) -> term().
%% ====================================================================
update_user_files_size_view(ProtocolVersion) ->
    case dao_lib:apply(dao_users, update_files_size, [], ProtocolVersion) of
        ok ->
            ?debug("User files size view updated"),
            ok;
        Other ->
            ?error("Error during updating user files size view: ~p", [Other]),
            Other
    end.

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
    Attr :: atime | mtime | ctime | size | times | acl | xattr_set| xattr_remove.
%% ====================================================================
update_meta_attr(File, Attr, Value) ->
    update_meta_attr(File, Attr, Value, false).
update_meta_attr(File, Attr, Value, ForceSynch) ->
    update_meta_attr(File, Attr, Value, 5, ForceSynch).

%% update_parent_ctime/2
%% ====================================================================
%% @doc Sends #updatetimes request for parent of given directory.
-spec update_parent_ctime(Dir :: string(), CTime :: non_neg_integer()) -> ok | error.
%% ====================================================================
update_parent_ctime(Dir, CTime) ->
    case fslogic_path:strip_path_leaf(Dir) of
        [?PATH_SEPARATOR] -> ok;
        ParentPath ->
            try
                gen_server:call(?Dispatcher_Name, {fslogic, 1, #worker_request{subject = fslogic_context:get_user_dn(), request = {internal_call, #updatetimes{file_logic_name = ParentPath, mtime = CTime}}}})
            catch
                E1:E2 ->
                    ?error("update_parent_ctime error: ~p:~p", [E1, E2]),
                    error
            end
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% update_meta_attr/5
%% ====================================================================
%% @doc Internal implementation of update_meta_attr/3. See update_meta_attr/3 for more information.
%% @end
-spec update_meta_attr(File :: #file{}, Attr, Value :: term(), RetryCount :: integer(), ForceSynch :: boolean()) -> Result :: #file{} when
    Attr :: atime | mtime | ctime | size | times | acl | xattr_set| xattr_remove.
update_meta_attr(#file{meta_doc = MetaUUID} = File, Attr, Value, RetryCount, ForceSynch) ->
    {File1, #db_document{record = MetaRec} = MetaDoc} = init_file_meta(File),
    MetaDocChanged = MetaUUID =/= MetaDoc#db_document.uuid,
    FileOwnerChanged = File#file.uid =/= MetaRec#file_meta.uid,
    RunSync = MetaDocChanged or FileOwnerChanged,
    SyncTask = fun() ->
        NewMeta =
            case Attr of
                times ->
                    case Value of
                        {ATime, MTime, CTime}                    -> MetaRec#file_meta{uid = File#file.uid,
                                                                        atime = max(ATime, MetaRec#file_meta.atime),
                                                                        mtime = max(MTime, MetaRec#file_meta.mtime),
                                                                        ctime = max(CTime, MetaRec#file_meta.ctime)};
                        {ATime, MTime} when ATime > 0, MTime > 0 -> MetaRec#file_meta{uid = File#file.uid,
                                                                        atime = max(ATime, MetaRec#file_meta.atime),
                                                                        mtime = max(MTime, MetaRec#file_meta.mtime)};
                        {ATime, _MTime} when ATime > 0           -> MetaRec#file_meta{uid = File#file.uid, atime = max(ATime, MetaRec#file_meta.atime)};
                        {_ATime, MTime} when MTime > 0           -> MetaRec#file_meta{uid = File#file.uid, mtime = max(MTime, MetaRec#file_meta.mtime)}
                    end;
                ctime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, ctime = max(Value, MetaRec#file_meta.ctime)};
                mtime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, mtime = max(Value, MetaRec#file_meta.mtime)};
                atime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, atime = max(Value, MetaRec#file_meta.atime)};
                size when Value >= 0 -> MetaRec#file_meta{uid = File#file.uid, size = Value};
                acl -> MetaRec#file_meta{uid = File#file.uid, acl = Value};
                xattr_set ->
                    {Key, Val} = Value,
                    MetaRec#file_meta{uid = File#file.uid, xattrs = [{Key, Val} | proplists:delete(Key, MetaRec#file_meta.xattrs)]};
                xattr_remove -> MetaRec#file_meta{uid = File#file.uid, xattrs = proplists:delete(Value, MetaRec#file_meta.xattrs)};
                _ -> MetaRec
            end,
        case MetaRec of
            NewMeta -> File1;
            _ ->
                NewDoc = MetaDoc#db_document{record = NewMeta},
                case dao_lib:apply(dao_vfs, save_file_meta, [NewDoc], 1) of
                    {ok, _} -> File1;
                    {error, conflict} when RetryCount > 0 ->
                        ?warning("Conflict when saveing file_meta record for file (name = ~p, parent = ~p). Retring...", [File#file.name, File#file.parent]),
                        {_, _, M} = now(),
                        timer:sleep(M rem ?MAX_SLEEP_TIME_CONFLICT_RESOLUTION), %% If case of conflict we should wait a little bit before next try (max 100ms)
                        update_meta_attr(File1, Attr, Value, RetryCount - 1, ForceSynch);
                    {error, Error} ->
                        ?warning("Cannot save file_meta record for file (name = ~p, parent = ~p) due to error: ~p", [File#file.name, File#file.parent, Error])
                end
        end
    end, %% SyncTask = fun()
    case (ForceSynch orelse RunSync) of
        true -> SyncTask();
        false ->
            spawn(SyncTask),
            File
    end.


%% init_file_meta/1
%% ====================================================================
%% @doc Internal implementation of update_meta_attr/3. This method handles creation of not existing #file_meta document.
%% @end
-spec init_file_meta(File :: #file{}) -> Result :: {#file{}, term()}.
%% ====================================================================
init_file_meta(#file{meta_doc = MetaUUID} = File) when is_list(MetaUUID) ->
    case dao_lib:apply(dao_vfs, get_file_meta, [MetaUUID], 1)  of
        {ok, #db_document{} = MetaDoc} -> {File, MetaDoc};
        Error ->
            ?error("File (name = ~p, parent = ~p) points to file_meta (uuid = ~p) that is not available. DAO response: ~p", [File#file.name, File#file.parent, MetaUUID, Error]),
            {File, #db_document{uuid = MetaUUID, record = #file_meta{}}}
    end;
init_file_meta(#file{} = File) ->
    case dao_lib:apply(dao_vfs, save_file_meta, [#file_meta{uid = File#file.uid}], 1) of
        {ok, MetaUUID} when is_list(MetaUUID) -> init_file_meta(File#file{meta_doc = MetaUUID});
        Error ->
            ?error("Cannot save file_meta record for file (name = ~p, parent = ~p) due to: ~p", [File#file.name, File#file.parent, Error]),
            {File, undefined}
    end.
