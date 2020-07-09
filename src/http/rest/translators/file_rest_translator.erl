%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% file entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(file_rest_translator).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").

-export([create_response/4, get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(gri:gri(), middleware:auth_hint(),
    middleware:data_format(), Result :: term() | {gri:gri(), term()} |
    {gri:gri(), middleware:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = object_id}, _, value, ObjectId) ->
    ?OK_REPLY(#{<<"fileId">> => ObjectId});
create_response(#gri{aspect = register_file}, _, value, ObjectId) ->
    ?CREATED_REPLY([<<"data">>, ObjectId],  #{<<"fileId">> => ObjectId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{aspect = As}, Result) when
    As =:= object_id;
    As =:= list
->
    ?OK_REPLY(Result);
get_response(#gri{aspect = children}, Children) ->
    ?OK_REPLY(#{<<"children">> => lists:map(fun({Guid, Name}) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        #{
            <<"id">> => ObjectId,
            <<"name">> => Name
        }
    end, Children)});
get_response(#gri{aspect = As}, Metadata) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata
->
    ?OK_REPLY(Metadata);
get_response(#gri{aspect = rdf_metadata}, RdfMetadata) ->
    ?OK_REPLY({binary, RdfMetadata});

get_response(#gri{aspect = As}, EffQosResp) when
    As =:= distribution;
    As =:= file_qos_summary
->
    ?OK_REPLY(EffQosResp).
