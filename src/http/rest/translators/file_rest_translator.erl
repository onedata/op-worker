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

-export([get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{aspect = list}, Result) ->
    ?OK_REPLY(Result);
get_response(#gri{aspect = As}, Metadata) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata
->
    ?OK_REPLY(Metadata);
get_response(#gri{aspect = rdf_metadata}, RdfMetadata) ->
    ?OK_REPLY({binary, RdfMetadata}).
