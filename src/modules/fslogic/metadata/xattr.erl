%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API for files' extended attributes.
%%% @end
%%%-------------------------------------------------------------------
-module(xattr).
-author("Tomasz Lichon").

-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([list/4, get/4, set/6, remove/3]).
%% Protected API - for use only by *_req.erl modules
-export([list_xattrs_insecure/4]).


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
        [traverse_ancestors]
    ),
    list_xattrs_insecure(UserCtx, FileCtx1, IncludeInherited, ShowInternal).


-spec get(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:name(),
    Inherited :: boolean()
) ->
    {ok, custom_metadata:value()} | {error, term()}.
get(UserCtx, FileCtx, XattrName, false) ->
    get_xattr(UserCtx, FileCtx, XattrName);
get(UserCtx, FileCtx, XattrName, true = Inherited) ->
    case get_xattr(UserCtx, FileCtx, XattrName) of
        {ok, _} = Result ->
            Result;
        ?ERROR_NOT_FOUND ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            {ParentCtx, _FileCtx1} = file_ctx:get_parent(FileCtx, UserCtx),

            case file_ctx:get_guid_const(ParentCtx) of
                FileGuid ->
                    % root dir/share root file -> there are no parents
                    ?ERROR_NOT_FOUND;
                _ ->
                    get(UserCtx, ParentCtx, XattrName, Inherited)
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
        [traverse_ancestors, ?write_metadata]
    ),
    custom_metadata:set_xattr(
        file_ctx:get_uuid_const(FileCtx1),
        file_ctx:get_space_id_const(FileCtx1),
        XattrName, XattrValue, Create, Replace
    ).


-spec remove(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:name()) ->
    ok | {error, term()}.
remove(UserCtx, FileCtx0, XattrName) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    custom_metadata:remove_xattr(FileUuid, XattrName).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec list_xattrs_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    IncludeInherited :: boolean(),
    ShowInternal :: boolean()
) ->
    {ok, [custom_metadata:name()]}.
list_xattrs_insecure(UserCtx, FileCtx, IncludeInherited, ShowInternal) ->
    {ok, DirectXattrs} = list_direct_xattrs(FileCtx),
    AllXattrs = case IncludeInherited of
        true ->
            {ok, AncestorsXattrs} = list_ancestor_xattrs(UserCtx, FileCtx, []),
            % Filter out cdmi attributes - those are not inherited
            InheritedXattrs = lists:filter(fun(Key) ->
                not is_cdmi_xattr(Key)
            end, AncestorsXattrs),
            lists:usort(DirectXattrs ++ InheritedXattrs);
        false ->
            DirectXattrs
    end,

    FilteredXattrs = case ShowInternal of
        true ->
            % acl is kept in file_meta instead of custom_metadata
            % but is still treated as metadata so ?ACL_KEY
            % is added to listing if active perms type for
            % file is acl. Otherwise, to keep backward compatibility,
            % it is omitted.
            case file_ctx:get_active_perms_type(FileCtx, ignore_deleted) of
                {acl, _} ->
                    [?ACL_KEY | AllXattrs];
                _ ->
                    AllXattrs
            end;
        false ->
            lists:filter(fun(Key) -> not is_internal_xattr(Key) end, AllXattrs)
    end,
    {ok, FilteredXattrs}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists names of all extended attributes set directly on given file.
%% @end
%%--------------------------------------------------------------------
-spec list_direct_xattrs(file_ctx:ctx()) ->
    {ok, [custom_metadata:name()]} | {error, term()}.
list_direct_xattrs(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
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
    FileGuid = file_ctx:get_guid_const(FileCtx0),
    {ParentCtx, _FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),

    case file_ctx:get_guid_const(ParentCtx) of
        FileGuid ->
            % root dir/share root file -> there are no parents
            {ok, GatheredXattrNames};
        _ ->
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
        [traverse_ancestors, ?read_metadata]
    ),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
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
