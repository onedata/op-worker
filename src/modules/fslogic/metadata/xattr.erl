%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API for files' extended attributes.
%%% Note: this module bases on custom_metadata and as effect all operations
%%% on hardlinks are treated as operations on original file (custom_metadata
%%% is shared between hardlinks and original file).
%%% @end
%%%-------------------------------------------------------------------
-module(xattr).
-author("Tomasz Lichon").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([list/4, get/4, set/6, remove/3]).
%% Protected API - for use only by *_req.erl modules
-export([list_insecure/4, get_all_direct_insecure/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(
    user_ctx:ctx(),
    file_ctx:ctx(),
    IncludeInherited :: boolean(),
    ShowInternal :: boolean()
) ->
    {ok, [custom_metadata:name()]}.
list(UserCtx, FileCtx0, IncludeInherited, ShowInternal) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS]
    ),
    list_insecure(UserCtx, FileCtx1, IncludeInherited, ShowInternal).


-spec get(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:name(),
    Inherited :: boolean()
) ->
    {ok, custom_metadata:value()} | {error, term()}.
get(UserCtx, FileCtx, XattrName, false) ->
    get_xattr(UserCtx, FileCtx, XattrName);
get(UserCtx, FileCtx0, XattrName, true = Inherited) ->
    case get_xattr(UserCtx, FileCtx0, XattrName) of
        {ok, _} = Result ->
            Result;
        ?ERROR_NOT_FOUND ->
            {ParentCtx, FileCtx1} = file_tree:get_parent(FileCtx0, UserCtx),

            case file_ctx:equals(FileCtx1, ParentCtx) of
                true -> ?ERROR_NOT_FOUND;
                false -> get(UserCtx, ParentCtx, XattrName, Inherited)
            end
    end.


-spec set(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:name(),
    custom_metadata:value(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    {ok, file_meta:uuid()} | {error, term()}.
set(UserCtx, FileCtx0, XattrName, XattrValue, Create, Replace) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_metadata_mask)]
    ),
    {IsIgnoredInChanges, _FileCtx2} = file_ctx:is_ignored_in_changes(FileCtx1),
    custom_metadata:set_xattr(
        file_ctx:get_logical_uuid_const(FileCtx1),
        file_ctx:get_space_id_const(FileCtx1),
        XattrName, XattrValue, Create, Replace, IsIgnoredInChanges
    ).


-spec remove(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:name()) ->
    ok | {error, term()}.
remove(UserCtx, FileCtx0, XattrName) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_metadata_mask)]
    ),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx1),
    custom_metadata:remove_xattr(FileUuid, XattrName).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec list_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    IncludeInherited :: boolean(),
    ShowInternal :: boolean()
) ->
    {ok, [custom_metadata:name()]}.
list_insecure(UserCtx, FileCtx, IncludeInherited, ShowInternal) ->
    {ok, DirectXattrNames} = list_direct_xattrs(FileCtx),
    XattrNames = case IncludeInherited of
        true ->
            {ok, AncestorsXattrs} = list_ancestor_xattrs(UserCtx, FileCtx, []),
            % Filter out cdmi attributes - those are not inherited
            InheritedXattrs = lists:filter(fun(Key) ->
                not is_cdmi_xattr(Key)
            end, AncestorsXattrs),
            lists:usort(DirectXattrNames ++ InheritedXattrs);
        false ->
            DirectXattrNames
    end,
    case ShowInternal of
        true -> {ok, prepend_acl(FileCtx, XattrNames)};
        false -> {ok, filter_internal(XattrNames)}
    end.


%% @private
-spec get_all_direct_insecure(file_ctx:ctx()) ->
    {ok, #{custom_metadata:name() => custom_metadata:value()}} | {error, term()}.
get_all_direct_insecure(FileCtx) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    case custom_metadata:get_all_xattrs(FileUuid) of
        {ok, AllXattrsWithValues} ->
            {ok, maps:with(filter_internal(maps:keys(AllXattrsWithValues)), AllXattrsWithValues)};
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists names of all extended attributes set directly on given file.
%% @end
%%--------------------------------------------------------------------
-spec list_direct_xattrs(file_ctx:ctx()) ->
    {ok, [custom_metadata:name()]} | {error, term()}.
list_direct_xattrs(FileCtx) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    custom_metadata:list_xattrs(FileUuid).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Traverses file ancestors to gather inherited extended attributes.
%% @end
%%--------------------------------------------------------------------
-spec list_ancestor_xattrs(user_ctx:ctx(), file_ctx:ctx(), [custom_metadata:name()]) ->
    {ok, [custom_metadata:name()]} | {error, term()}.
list_ancestor_xattrs(UserCtx, FileCtx0, GatheredXattrNames) ->
    {ParentCtx, FileCtx1} = file_tree:get_parent(FileCtx0, UserCtx),

    case file_ctx:equals(FileCtx1, ParentCtx) of
        true ->
            {ok, GatheredXattrNames};
        false ->
            AllXattrNames = case list_direct_xattrs(FileCtx0) of
                {ok, []} ->
                    GatheredXattrNames;
                {ok, XattrNames} ->
                    lists:usort(XattrNames ++ GatheredXattrNames)
            end,
            list_ancestor_xattrs(UserCtx, ParentCtx, AllXattrNames)
    end.


%% @private
-spec get_xattr(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:name()) ->
    {ok, custom_metadata:value()} | {error, term()}.
get_xattr(UserCtx, FileCtx0, XattrName) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_metadata_mask)]
    ),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx1),
    custom_metadata:get_xattr(FileUuid, XattrName).


%% @private
-spec is_internal_xattr(custom_metadata:name()) -> boolean().
is_internal_xattr(XattrName) ->
    lists:any(fun(InternalPrefix) ->
        str_utils:binary_starts_with(XattrName, InternalPrefix)
    end, ?METADATA_INTERNAL_PREFIXES).


%% @private
-spec is_cdmi_xattr(custom_metadata:name()) -> boolean().
is_cdmi_xattr(XattrName) ->
    str_utils:binary_starts_with(XattrName, ?CDMI_PREFIX).


%% @private
-spec prepend_acl(file_ctx:ctx(), [custom_metadata:name()]) -> 
    [custom_metadata:name()].
prepend_acl(FileCtx, XattrsKeys) ->
    % acl is kept in file_meta instead of custom_metadata
    % but is still treated as metadata so ?ACL_KEY
    % is added to listing if active perms type for
    % file is acl. Otherwise, to keep backward compatibility,
    % it is omitted.
    case file_ctx:get_active_perms_type(FileCtx, ignore_deleted) of
        {acl, _} ->
            [?ACL_KEY | XattrsKeys];
        _ ->
            XattrsKeys
    end.


%% @private
-spec filter_internal([custom_metadata:name()]) ->
    [custom_metadata:name()].
filter_internal(XattrsKeys) ->
    lists:filter(fun(Key) -> not is_internal_xattr(Key) end, XattrsKeys).
