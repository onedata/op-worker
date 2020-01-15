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
translate_value(#gri{aspect = transfers}, TransfersForFile) ->
    TransfersForFile;
translate_value(#gri{aspect = download_url}, URL) ->
    #{<<"fileUrl">> => URL}.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{id = Guid, aspect = instance, scope = private}, #file_attr{
    name = Name,
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

    fun(#auth{session_id = SessId}) ->
        FileUuid = file_id:guid_to_uuid(Guid),
        {ok, ActivePermsType} = file_meta:get_active_perms_type(FileUuid),

        Parent = case fslogic_uuid:is_space_dir_guid(Guid) of
            true ->
                null;
            false ->
                {ok, ParentGuid} = ?check(lfm:get_parent(SessId, {guid, Guid})),
                gri:serialize(#gri{
                    type = op_file,
                    id = ParentGuid,
                    aspect = instance,
                    scope = private
                })
        end,

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
            <<"name">> => Name,
            <<"owner">> => gri:serialize(#gri{
                type = op_user,
                id = Owner,
                aspect = instance,
                scope = shared
            }),
            <<"index">> => Name,
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
            <<"share">> => SharesGRI,
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
