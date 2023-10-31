%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(attr_req).
-author("Tomasz Lichon").

-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    get_file_attr/3, 
    get_file_attr_by_path/4,
    get_file_references/2,
    get_child_attr/4, chmod/3, update_times/5,
    get_fs_stats/2,
    optional_attrs_privs_mask/1
]).

%% Protected API (for use only by *_req level modules)
-export([
    get_file_attr_insecure/3,
    chmod_attrs_only_insecure/2
]).

-type attribute() :: file_attr:attribute().


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv get_file_attr_insecure/3 with permission checks and default options
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(user_ctx:ctx(), file_ctx:ctx(), file_attr:resolve_opts() | [attribute()]) ->
    fslogic_worker:fuse_response().
get_file_attr(UserCtx, FileCtx0, Options) when is_map(Options) ->
    RequiredPrivs = [?TRAVERSE_ANCESTORS, ?OPERATIONS(optional_attrs_privs_mask(Options))],
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, RequiredPrivs, allow_ancestors
    ),
    get_file_attr_insecure(UserCtx, FileCtx1, Options);
get_file_attr(UserCtx, FileCtx, Attributes) when is_list(Attributes) ->
    get_file_attr(UserCtx, FileCtx, #{attributes => Attributes}).


-spec get_file_attr_by_path(user_ctx:ctx(), file_ctx:ctx(), file_meta:path(), [attribute()]) ->
    fslogic_worker:fuse_response().
get_file_attr_by_path(UserCtx, RootFileCtx, Path, Attributes) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        guid_req:resolve_guid_by_relative_path(UserCtx, RootFileCtx, Path),
    get_file_attr(UserCtx, file_ctx:new_by_guid(Guid), Attributes).


-spec get_file_references(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_references(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_attributes_mask)]
    ),
    FileCtx2 = file_ctx:assert_not_dir(FileCtx1),

    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {ok, RefUuids} = file_ctx:list_references_const(FileCtx2),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_references{references = lists:map(fun(RefUuid) ->
            file_id:pack_guid(RefUuid, SpaceId)
        end, RefUuids)}
    }.


%%--------------------------------------------------------------------
%% @equiv get_child_attr_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name(), [attribute()]) -> fslogic_worker:fuse_response().
get_child_attr(UserCtx, ParentFileCtx0, Name, Attributes) ->
    ParentFileCtx1 = ensure_access_to_child(UserCtx, ParentFileCtx0, Name),
    get_child_attr_insecure(
        UserCtx, ParentFileCtx1, Name, Attributes
    ).


%%--------------------------------------------------------------------
%% @equiv chmod_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec chmod(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
chmod(UserCtx, FileCtx0, Mode) ->
    file_ctx:assert_not_special_const(FileCtx0),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OWNERSHIP]
    ),
    chmod_insecure(UserCtx, FileCtx1, Mode).


%%--------------------------------------------------------------------
%% @equiv update_times_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec update_times(user_ctx:ctx(), file_ctx:ctx(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> fslogic_worker:fuse_response().
update_times(UserCtx, FileCtx0, ATime, MTime, CTime) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OR(?OWNERSHIP, ?OPERATIONS(?write_attributes_mask))]
    ),
    update_times_insecure(UserCtx, FileCtx1, ATime, MTime, CTime).


%%--------------------------------------------------------------------
%% @equiv get_fs_stats/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_fs_stats(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_fs_stats(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]
    ),
    get_fs_stats_insecure(UserCtx, FileCtx1).


-spec optional_attrs_privs_mask([attribute()] | file_attr:resolve_opts()) -> data_access_control:bitmask().
optional_attrs_privs_mask(#{attributes := AttributesList}) ->
    optional_attrs_privs_mask(AttributesList);
optional_attrs_privs_mask(AttributesList) ->
    Metadata = case file_attr:should_fetch_xattrs(AttributesList) of
        {true, _} -> ?read_metadata_mask;
        false -> 0
    end,
    case lists:member(acl, AttributesList) of
        true -> Metadata bor ?read_acl_mask;
        false -> Metadata
    end.


%%%===================================================================
%%% Protected API (for use only by *_req level modules)
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes depending on specific flags set.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_insecure(user_ctx:ctx(), file_ctx:ctx(), file_attr:resolve_opts()) ->
    fslogic_worker:fuse_response().
get_file_attr_insecure(UserCtx, FileCtx, Opts) ->
    {FileAttr, _FileCtx2} = file_attr:resolve(UserCtx, FileCtx, Opts),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = FileAttr
    }.


%%--------------------------------------------------------------------
%% @doc
%% Changes file permissions (only file_attrs, not on storage)
%% @end
%%--------------------------------------------------------------------
-spec chmod_attrs_only_insecure(file_ctx:ctx(),
    fslogic_worker:posix_permissions()) -> file_ctx:ctx().
chmod_attrs_only_insecure(FileCtx, Mode) ->
    % TODO VFS-7524 - verify if file_meta doc updates invalidate cached docs in file_ctx everywhere
    % TODO VFS-7525 - Protect races on events production after parallel file_meta updates
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    {ok, NewDoc} = file_meta:update_mode(FileUuid, Mode),
    ok = permissions_cache:invalidate(),
    FileCtx2 = file_ctx:set_file_doc(FileCtx, NewDoc),
    fslogic_event_emitter:emit_sizeless_file_attrs_changed(FileCtx2),
    fslogic_event_emitter:emit_file_perm_changed(FileCtx2),
    FileCtx2.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec ensure_access_to_child(user_ctx:ctx(), file_ctx:ctx(), file_meta:name()) ->
    file_ctx:ctx().
ensure_access_to_child(UserCtx, ParentFileCtx0, ChildName) ->
    case fslogic_authz:ensure_authorized_readdir(
        UserCtx, ParentFileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask)]
    ) of
        {undefined, ParentFileCtx1} ->
            ParentFileCtx1;
        {CanonicalChildrenWhitelist, ParentFileCtx1} ->
            CanonicalChildName = case file_ctx:is_user_root_dir_const(ParentFileCtx1, UserCtx) of
                true ->
                    SessionId = user_ctx:get_session_id(UserCtx),
                    UserDoc = user_ctx:get_user(UserCtx),

                    case user_logic:get_space_by_name(SessionId, UserDoc, ChildName) of
                        {true, SpaceId} -> SpaceId;
                        false -> throw(?ENOENT)
                    end;
                false ->
                    ChildName
            end,
            lists:member(CanonicalChildName, CanonicalChildrenWhitelist) orelse throw(?ENOENT),
            ParentFileCtx1
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns attributes of directory child (if exists).
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr_insecure(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name(), [attribute()]) -> fslogic_worker:fuse_response().
get_child_attr_insecure(UserCtx, ParentFileCtx, Name, Attributes) ->
    {ChildFileCtx, _NewParentFileCtx} = file_tree:get_child(ParentFileCtx, Name, UserCtx),
    Response = get_file_attr(UserCtx, ChildFileCtx, Attributes),
    ensure_proper_file_name(Response, Name).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that requested Name is in returned #file_attr{} if it has no suffix.
%% Used to prevent races connected with remote rename.
%% @end
%%-------------------------------------------------------------------
-spec ensure_proper_file_name(fslogic_worker:fuse_response(), file_meta:name()) ->
    fslogic_worker:fuse_response().
ensure_proper_file_name(FuseResponse = #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{name = AnsName} = FileAttr
}, Name) ->
    case file_meta:has_suffix(Name) of
        {true, AnsName} -> FuseResponse;
        _ -> FuseResponse#fuse_response{fuse_response = FileAttr#file_attr{name = Name}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Changes file posix mode.
%% @end
%%--------------------------------------------------------------------
-spec chmod_insecure(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
chmod_insecure(UserCtx, FileCtx, Mode) ->
    sd_utils:chmod(UserCtx, FileCtx, Mode),
    FileCtx2 = chmod_attrs_only_insecure(FileCtx, Mode),
    fslogic_times:update_ctime(FileCtx2),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Changes file access times.
%% @end
%%--------------------------------------------------------------------
-spec update_times_insecure(user_ctx:ctx(), file_ctx:ctx(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> fslogic_worker:fuse_response().
update_times_insecure(UserCtx, FileCtx, ATime, MTime, CTime) ->
    % Flush events queue to prevent race with file_written_event
    % TODO VFS-7139: This is temporary solution to be removed after fixing oneclient
    SessId = user_ctx:get_session_id(UserCtx),
    catch lfm_event_controller:flush_event_queue(
        SessId, oneprovider:get_id(), file_ctx:get_logical_guid_const(FileCtx)),

    TimesDiff1 = fun
        (Times = #times{}) when ATime == undefined -> Times;
        (Times = #times{}) -> Times#times{atime = ATime}
    end,
    TimesDiff2 = fun
        (Times = #times{}) when MTime == undefined -> Times;
        (Times = #times{}) -> Times#times{mtime = MTime}
    end,
    TimesDiff3 = fun
        (Times = #times{}) when CTime == undefined -> Times;
        (Times = #times{}) -> Times#times{ctime = CTime}
    end,
    TimesDiff = fun(Times = #times{}) ->
        {ok, TimesDiff1(TimesDiff2(TimesDiff3(Times)))}
    end,
    fslogic_times:update_times_and_emit(FileCtx, TimesDiff),
    #fuse_response{status = #status{code = ?OK}}.


%% @private
-spec get_fs_stats_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_fs_stats_insecure(_UserCtx, FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    
    %% @TODO VFS-5497 Calc size/occupied for all supporting storages
    {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
    {ok, SupportSize} = provider_logic:get_support_size(SpaceId),
    Occupied = space_quota:current_size(SpaceId),
    
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #fs_stats{
            space_id = SpaceId,
            storage_stats = [#storage_stats{
                storage_id = StorageId,
                size = SupportSize,
                occupied = Occupied
            }]
        }
    }.
