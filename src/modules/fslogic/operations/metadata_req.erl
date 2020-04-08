%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file's
%%% metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(metadata_req).
-author("Tomasz Lichon").

-include("modules/fslogic/metadata.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([get_metadata/5, set_metadata/7, remove_metadata/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_metadata(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:type(),
    custom_metadata:filter(),
    Inherited :: boolean()
) ->
    fslogic_worker:provider_response().
get_metadata(UserCtx, FileCtx, Type, Filter, Inherited) ->
    Result = case Type of
        json -> json_metadata:get(UserCtx, FileCtx, Filter, Inherited);
        rdf -> xattr:get(UserCtx, FileCtx, ?RDF_METADATA_KEY, Inherited)
    end,
    case Result of
        {ok, Value} ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #metadata{type = Type, value = Value}
            };
        ?ERROR_NOT_FOUND ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.


-spec set_metadata(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:type(),
    custom_metadata:value(),
    custom_metadata:filter(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    fslogic_worker:provider_response().
set_metadata(UserCtx, FileCtx, json, Value, Filter, Create, Replace) ->
    {ok, _} = json_metadata:set(UserCtx, FileCtx, Value, Filter, Create, Replace),
    #provider_response{status = #status{code = ?OK}};
set_metadata(UserCtx, FileCtx, rdf, Value, _, Create, Replace) ->
    {ok, _} = xattr:set(UserCtx, FileCtx, ?RDF_METADATA_KEY, Value, Create, Replace),
    #provider_response{status = #status{code = ?OK}}.


-spec remove_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type()) ->
    fslogic_worker:provider_response().
remove_metadata(UserCtx, FileCtx, json) ->
    ok = json_metadata:remove(UserCtx, FileCtx),
    #provider_response{status = #status{code = ?OK}};
remove_metadata(UserCtx, FileCtx, rdf) ->
    ok = xattr:remove(UserCtx, FileCtx, ?RDF_METADATA_KEY),
    #provider_response{status = #status{code = ?OK}}.
