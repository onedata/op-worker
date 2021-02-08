%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% share entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(share_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = Scope}, #{
    <<"name">> := ShareName,
    <<"description">> := Description,
    <<"publicUrl">> := SharePublicUrl,
    <<"fileType">> := FileType,
    <<"rootFileId">> := RootFileShareGuid,
    <<"handleId">> := HandleId
}) when
    Scope =:= private;
    Scope =:= public
->
    fun(Auth) ->
        ShareInfo = case Scope of
            public ->
                #{};
            private ->
                RootFileGuid = file_id:share_guid_to_guid(RootFileShareGuid),
                #{<<"privateRootFile">> => gri:serialize(#gri{
                    type = op_file,
                    id = RootFileGuid,
                    aspect = instance,
                    scope = private
                })}
        end,

        HandleGri = case HandleId of
            null ->
                null;
            _ ->
                HandleScope = case {Scope, Auth} of
                    {private, ?USER(UserId, SessionId)} ->
                        case handle_logic:has_eff_user(SessionId, HandleId, UserId) of
                            true -> private;
                            false -> public
                        end;
                    {_, _} ->
                        public
                end,
                gri:serialize(#gri{
                    type = op_handle,
                    id = HandleId,
                    aspect = instance,
                    scope = HandleScope
                })
        end,

        ShareInfo#{
            <<"name">> => ShareName,
            <<"description">> => Description,
            <<"publicUrl">> => SharePublicUrl,
            <<"fileType">> => FileType,
            <<"rootFile">> => gri:serialize(#gri{
                type = op_file,
                id = RootFileShareGuid,
                aspect = instance,
                scope = public
            }),
            <<"handle">> => HandleGri
        }
    end.
