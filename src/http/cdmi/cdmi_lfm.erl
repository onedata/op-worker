%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements utility functions for use by other `cdmi_` modules.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_lfm).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").

%% API
-export([create_file/3, create_dir/2, cp/3, mv/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_file(session:id(), file_meta:path(), undefined | file_meta:posix_permissions()) ->
    {ok, file_id:file_guid()} | no_return().
create_file(SessionId, Path, Mode) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),
    {ok, ParentGuid} = middleware_utils:resolve_file_path(SessionId, ParentPath),
    ?check(lfm:create(SessionId, ParentGuid, Name, Mode)).


-spec create_dir(session:id(), file_meta:path()) ->
    {ok, file_id:file_guid()} | no_return().
create_dir(SessionId, Path) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),
    {ok, ParentGuid} = middleware_utils:resolve_file_path(SessionId, ParentPath),
    ?check(lfm:mkdir(SessionId, ParentGuid, Name, undefined)).


-spec cp(session:id(), file_meta:path(), file_meta:path()) ->
    {ok, file_id:file_guid()} | no_return().
cp(SessionId, SrcURI, DstURI) ->
    {ok, SrcGuid} = middleware_utils:resolve_file_path(
        SessionId, filepath_utils:ensure_begins_with_slash(SrcURI)
    ),
    {DstName, DstParentPath} = filepath_utils:basename_and_parent_dir(DstURI),
    {ok, DstParentGuid} = middleware_utils:resolve_file_path(SessionId, DstParentPath),

    ?check(lfm:cp(SessionId, {guid, SrcGuid}, {guid, DstParentGuid}, DstName)).


-spec mv(session:id(), file_meta:path(), file_meta:path()) ->
    {ok, file_id:file_guid()} | no_return().
mv(SessionId, SrcURI, DstURI) ->
    {ok, SrcGuid} = middleware_utils:resolve_file_path(
        SessionId, filepath_utils:ensure_begins_with_slash(SrcURI)
    ),
    {DstName, DstParentPath} = filepath_utils:basename_and_parent_dir(DstURI),
    {ok, DstParentGuid} = middleware_utils:resolve_file_path(SessionId, DstParentPath),

    ?check(lfm:mv(SessionId, {guid, SrcGuid}, {guid, DstParentGuid}, DstName)).
