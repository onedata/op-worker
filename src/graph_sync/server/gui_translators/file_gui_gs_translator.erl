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
        translate_file_details(ChildDetails, Scope)
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
translate_resource(#gri{aspect = instance, scope = Scope}, FileDetails) ->
    translate_file_details(FileDetails, Scope);
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
-spec translate_file_details(#file_details{}, gri:scope()) -> map().
translate_file_details(#file_details{
    has_metadata = HasMetadata,
    active_permissions_type = ActivePermissionsType,
    index_startid = StartId,
    file_attr = #file_attr{
        guid = FileGuid,
        name = FileName,
        mode = Mode,
        parent_uuid = ParentGuid,
        mtime = MTime,
        type = TypeAttr,
        size = SizeAttr,
        shares = Shares,
        provider_id = ProviderId,
        owner_id = OwnerId
    }
}, Scope) ->
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, SizeAttr}
    end,
    IsRootDir = case file_id:guid_to_share_id(FileGuid) of
        undefined ->
            fslogic_uuid:is_space_dir_guid(FileGuid);
        ShareId ->
            lists:member(ShareId, Shares)
    end,
    ParentId = case IsRootDir of
        true -> null;
        false -> ParentGuid
    end,
    PublicFields = #{
        <<"hasMetadata">> => HasMetadata,
        <<"activePermissionsType">> => ActivePermissionsType,
        <<"guid">> => FileGuid,
        <<"name">> => FileName,
        <<"index">> => StartId,
        <<"posixPermissions">> => integer_to_binary(Mode, 8),
        <<"parentId">> => ParentId,
        <<"mtime">> => MTime,
        <<"type">> => Type,
        <<"size">> => Size,
        <<"shares">> => Shares
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
