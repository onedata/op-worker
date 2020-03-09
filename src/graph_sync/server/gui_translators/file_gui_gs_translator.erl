%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% file entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(file_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = children}, Children) ->
    #{<<"children">> => lists:map(fun({Guid, _Name}) -> Guid end, Children)};
translate_value(#gri{aspect = children_details, scope = Scope}, ChildrenDetails) ->
    #{<<"children">> => lists:map(fun(ChildDetails) ->
        translate_file(ChildDetails, Scope)
    end, ChildrenDetails)};
translate_value(#gri{aspect = As}, Metadata) when
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    #{<<"metadata">> => Metadata};
translate_value(#gri{aspect = transfers}, TransfersForFile) ->
    TransfersForFile;
translate_value(#gri{aspect = download_url}, URL) ->
    #{<<"fileUrl">> => URL}.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{id = Guid, aspect = instance, scope = public}, #file_attr{
    name = FileName,
    type = TypeAttr,
    mode = Mode,
    size = SizeAttr,
    mtime = ModificationTime,
    shares = Shares
}) ->
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, SizeAttr}
    end,
    {_, SpaceId, ShareId} = file_id:unpack_share_guid(Guid),
    IsSpaceDir = fslogic_uuid:is_space_dir_guid(Guid),

    fun(#auth{session_id = SessId}) ->
        DisplayedName = case IsSpaceDir of
            true ->
                case space_logic:get_name(SessId, SpaceId) of
                    {ok, SpaceName} -> SpaceName;
                    {error, _} = Error -> throw(Error)
                end;
            false ->
                FileName
        end,
        IsRootDir = case ShareId of
            undefined ->
                fslogic_uuid:is_space_dir_guid(Guid);
            _ ->
                lists:member(ShareId, Shares)
        end,
        Parent = case IsRootDir of
            true ->
                null;
            false ->
                {ok, ParentGuid} = ?check(lfm:get_parent(SessId, {guid, Guid})),
                gri:serialize(#gri{
                    type = op_file,
                    id = ParentGuid,
                    aspect = instance,
                    scope = public
                })
        end,
        {ok, HasMetadata} = ?check(lfm:has_custom_metadata(
            SessId, {guid, Guid}
        )),

        #{
            <<"name">> => DisplayedName,
            <<"index">> => FileName,
            <<"type">> => Type,
            <<"posixPermissions">> => integer_to_binary((Mode rem 8#1000), 8),
            <<"mtime">> => ModificationTime,
            <<"size">> => Size,
            <<"hasMetadata">> => HasMetadata,
            <<"parent">> => Parent
        }
    end;
translate_resource(#gri{id = Guid, aspect = instance, scope = private}, #file_attr{
    name = FileName,
    owner_id = Owner,
    type = TypeAttr,
    mode = Mode,
    size = SizeAttr,
    mtime = ModificationTime,
    shares = Shares
}) ->
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, SizeAttr}
    end,
    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),

    fun(#auth{session_id = SessId}) ->
        {ok, ActivePermsType} = file_meta:get_active_perms_type(FileUuid),

        {Parent, DisplayedName} = case fslogic_uuid:is_space_dir_guid(Guid) of
            true ->
                case space_logic:get_name(SessId, SpaceId) of
                    {ok, SpaceName} ->
                        {null, SpaceName};
                    {error, _} = Error ->
                        throw(Error)
                end;
            false ->
                {ok, ParentGuid} = ?check(lfm:get_parent(SessId, {guid, Guid})),
                ParentGRI = gri:serialize(#gri{
                    type = op_file,
                    id = ParentGuid,
                    aspect = instance,
                    scope = private
                }),
                {ParentGRI, FileName}
        end,
        {ok, HasMetadata} = ?check(lfm:has_custom_metadata(
            SessId, {guid, Guid}
        )),
        SharesGRI = case Shares of
            [] ->
                null;
            _ ->
                gri:serialize(#gri{
                    type = op_file,
                    id = Guid,
                    aspect = shares,
                    scope = private
                })
        end,

        #{
            <<"name">> => DisplayedName,
            <<"owner">> => gri:serialize(#gri{
                type = op_user,
                id = Owner,
                aspect = instance,
                scope = shared
            }),
            <<"index">> => FileName,
            <<"type">> => Type,
            <<"posixPermissions">> => integer_to_binary((Mode rem 8#1000), 8),
            <<"activePermissionsType">> => ActivePermsType,
            <<"acl">> => gri:serialize(#gri{
                type = op_file,
                id = Guid,
                aspect = acl,
                scope = private
            }),
            <<"mtime">> => ModificationTime,
            <<"size">> => Size,
            <<"hasMetadata">> => HasMetadata,
            <<"shareList">> => SharesGRI,
            <<"distribution">> => gri:serialize(#gri{
                type = op_replica,
                id = Guid,
                aspect = distribution,
                scope = private
            }),
            <<"parent">> => Parent
        }
    end;
translate_resource(#gri{aspect = acl, scope = private}, Acl) ->
    try
        #{
            <<"list">> => acl:to_json(Acl, gui)
        }
    catch throw:{error, Errno} ->
        throw(?ERROR_POSIX(Errno))
    end;
translate_resource(#gri{aspect = shares, scope = private}, ShareIds) ->
    #{
        <<"list">> => lists:map(fun(ShareId) ->
            gri:serialize(#gri{
                type = op_share,
                id = ShareId,
                aspect = instance,
                scope = private
            })
        end, ShareIds)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec translate_file(#file_details{}, gri:scope()) -> map().
translate_file(#file_details{
    guid = FileGuid,
    name = FileName,
    active_permissions_type = ActivePermissionsType,
    mode = Mode,
    parent_guid = ParentGuid,
    mtime = MTime,
    type = Type,
    size = Size,
    shares = Shares,
    provider_id = ProviderId,
    owner_id = OwnerId,
    has_metadata = HasMetadata
}, Scope) ->
    PublicFields = #{
        <<"guid">> => FileGuid,
        <<"index">> => FileName,
        <<"name">> => FileName,
        <<"activePermissionsType">> => ActivePermissionsType,
        <<"mode">> => Mode,
        <<"parentId">> => ParentGuid,
        <<"mtime">> => MTime,
        <<"type">> => Type,
        <<"size">> => Size,
        <<"shares">> => Shares,
        <<"hasMetadata">> => HasMetadata
    },
    case Scope of
        public ->
            PublicFields;
        private ->
            PublicFields#{
                <<"providerId">> => ProviderId,
                <<"ownerId">> => OwnerId
            }
    end.
