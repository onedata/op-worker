%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour implements gs_translator_behaviour and is used to translate
%%% Graph Sync request results into format understood by GUI client.
%%% @end
%%%-------------------------------------------------------------------
-module(gui_gs_translator).
-author("Bartosz Walkowicz").

-behaviour(gs_translator_behaviour).

-include("op_logic.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([handshake_attributes/1, translate_value/3, translate_resource/3]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link gs_translator_behaviour} callback handshake_attributes/1.
%% @end
%%--------------------------------------------------------------------
-spec handshake_attributes(aai:auth()) ->
    gs_protocol:handshake_attributes().
handshake_attributes(_Client) ->
    {ok, ProviderName} = provider_logic:get_name(),

    #{
        <<"providerName">> => ProviderName,
        <<"serviceVersion">> => oneprovider:get_version(),
        <<"onezoneUrl">> => oneprovider:get_oz_url()
    }.


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_translator_behaviour} callback translate_value/3.
%% @end
%%--------------------------------------------------------------------
-spec translate_value(gs_protocol:protocol_version(), gs_protocol:gri(),
    Value :: term()) -> no_return().
translate_value(ProtocolVersion, GRI, Data) ->
    ?error("Cannot translate graph sync create result for:~n
    ProtocolVersion: ~p~n
    GRI: ~p~n
    Data: ~p~n", [
        ProtocolVersion, GRI, Data
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_translator_behaviour} callback translate_resource/3.
%% @end
%%--------------------------------------------------------------------
-spec translate_resource(gs_protocol:protocol_version(), gs_protocol:gri(),
    ResourceData :: term()) -> Result | fun((aai:auth()) -> Result) when
    Result :: gs_protocol:data() | gs_protocol:error() | no_return().
translate_resource(_, GRI = #gri{type = op_file}, Data) ->
    translate_file(GRI, Data);
translate_resource(_, GRI = #gri{type = op_space}, Data) ->
    translate_space(GRI, Data);
translate_resource(_, GRI = #gri{type = op_user}, Data) ->
    translate_user(GRI, Data);
translate_resource(ProtocolVersion, GRI, Data) ->
    ?error("Cannot translate graph sync get result for:~n
    ProtocolVersion: ~p~n
    GRI: ~p~n
    Data: ~p~n", [
        ProtocolVersion, GRI, Data
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec translate_file(gs_protocol:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_file(#gri{id = Guid, aspect = instance, scope = private}, #file_attr{
    name = Name,
    type = TypeAttr,
    size = SizeAttr,
    mtime = ModificationTime
}) ->
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, SizeAttr}
    end,

    fun(?USER(_UserId, SessId)) ->
        Parent = case fslogic_uuid:is_space_dir_guid(Guid) of
            true ->
                null;
            false ->
                {ok, ParentGuid} = ?check(lfm:get_parent(SessId, {guid, Guid})),
                gs_protocol:gri_to_string(#gri{
                    type = op_file,
                    id = ParentGuid,
                    aspect = instance,
                    scope = private
                })
        end,

        #{
            <<"name">> => Name,
            <<"index">> => Name,
            <<"type">> => Type,
            <<"mtime">> => ModificationTime,
            <<"size">> => Size,
            <<"parent">> => Parent
        }
    end.


%% @private
-spec translate_space(gs_protocol:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_space(#gri{id = SpaceId, aspect = instance, scope = private}, Space) ->
    RootDir = case space_logic:is_supported(Space, oneprovider:get_id()) of
        true ->
            Guid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
            gs_protocol:gri_to_string(#gri{
                type = op_file,
                id = Guid,
                aspect = instance,
                scope = private
            });
        false ->
            null
    end,

    #{
        <<"name">> => Space#od_space.name,
        <<"rootDir">> => RootDir
    }.


%% @private
-spec translate_user(gs_protocol:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_user(GRI = #gri{aspect = instance, scope = private}, #od_user{
    full_name = FullName,
    username = Username
}) ->
    #{
        <<"fullName">> => FullName,
        <<"username">> => gs_protocol:undefined_to_null(Username),
        <<"spaceList">> => gs_protocol:gri_to_string(GRI#gri{
            aspect = eff_spaces,
            scope = private
        })
    };
translate_user(#gri{aspect = eff_spaces, scope = private}, Spaces) ->
    #{
        <<"list">> => lists:map(fun(SpaceId) ->
            gs_protocol:gri_to_string(#gri{
                type = op_space,
                id = SpaceId,
                aspect = instance,
                scope = private
            })
        end, Spaces)
    }.