%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is a virtual directory (there are no associated documents in the db).
%%% It is being used in 'open_handle' mode. In that  mode listing space directory
%%% returns list of share root dirs instead of regular files/dirs in the space so
%%% that only shared content can be viewed (from this point down the tree the context
%%% is changed to shared one). In the future it will be used as mount root when
%%% mounting Oneclient for share with open handle (in such case it will be treated
%%% as root dir with no parent).
%%% @end
%%%-------------------------------------------------------------------
-module(share_root_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").


-export([
    uuid/1
]).

% special_dir_behaviour
-export([
    is_special/2,
    is_operation_allowed/1,
    exists/1,
    get_file_meta/1,
    get_times/2
]).

-define(SHARE_ROOT_DIR_UUID_PREFIX, "share_").

-define(ALLOWED_OPERATIONS, [
    resolve_guid,
    resolve_guid_by_relative_path,

    get_file_attr,
    get_file_children,
    get_child_attr,
    get_file_children_attrs,
    get_recursive_file_list
]).


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec uuid(od_share:id()) -> file_meta:uuid().
uuid(ShareId) ->
    <<?SHARE_ROOT_DIR_UUID_PREFIX, ShareId/binary>>.


-spec is_special(uuid | guid, file_meta:uuid() | file_id:file_guid()) -> boolean().
is_special(uuid, <<?SHARE_ROOT_DIR_UUID_PREFIX, _ShareId/binary>>) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec is_operation_allowed(atom()) -> boolean().
is_operation_allowed(Operation) ->
    lists:member(Operation, ?ALLOWED_OPERATIONS).


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    ShareId = share_root_dir:extract_share_id(Uuid),

    case share_logic:get(?ROOT_SESS_ID, ShareId) of
        {ok, _} -> true;
        ?ERROR_NOT_FOUND -> false
    end.


-spec get_file_meta(file_meta:uuid()) -> file_meta:doc().
get_file_meta(Uuid) ->
    ShareId = share_root_dir:extract_share_id(Uuid),

    #document{
        key = Uuid,
        value = #file_meta{
            name = ShareId,
            type = ?DIRECTORY_TYPE,
            is_scope = false,
            mode = ?DEFAULT_SHARE_ROOT_DIR_PERMS,
            owner = ?ROOT_USER_ID,
            provider_id = oneprovider:get_id(),
            deleted = case share_logic:get(?ROOT_SESS_ID, ShareId) of
                {ok, _} -> false;
                ?ERROR_NOT_FOUND -> true
            end
        }
    }.


-spec get_times(file_meta:uuid(), [times_api:times_type()]) -> times:record().
get_times(FileUuid, RequestedTimes) ->
    % Share root dir is virtual directory which does not have documents
    % like `file_meta` or `times` - in such case get times of share root file
    ShareId = share_root_dir:extract_share_id(FileUuid),
    {ok, #document{
        value = #od_share{
            root_file = RootFileShareGuid
        }
    }} = share_logic:get(?ROOT_SESS_ID, ShareId),

    RootFileGuid = file_id:share_guid_to_guid(RootFileShareGuid),
    times_api:get(file_ctx:new_by_guid(RootFileGuid), RequestedTimes).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec extract_share_id(file_meta:uuid()) -> od_share:id().
extract_share_id(<<?SHARE_ROOT_DIR_UUID_PREFIX, ShareId/binary>>) ->
    ShareId.
