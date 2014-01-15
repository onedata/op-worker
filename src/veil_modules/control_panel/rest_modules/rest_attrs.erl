%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements rest_module_behaviour and handles all
%% REST requests directed at /rest/attr/*. It returns file attributes,
%% if possible.
%% @end
%% ===================================================================

-module(rest_attrs).
-behaviour(rest_module_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/control_panel/rest_utils.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

-export([methods_and_versions_info/1, content_types_provided/3]).
-export([exists/3, get/3, delete/3, post/4, put/4]).


%% ====================================================================
%% Behaviour callback functions
%% ====================================================================


%% methods_and_versions_info/2
%% ====================================================================
%% @doc Should return list of tuples, where each tuple consists of version of API version and
%% list of methods available in the API version.
%% e.g.: `[{<<"1.0">>, [<<"GET">>, <<"POST">>]}]'
%% @end
-spec methods_and_versions_info(req()) -> {[{binary(), [binary()]}], req()}.
%% ====================================================================
methods_and_versions_info(Req, Id) ->
    {[{<<"1.0">>, [<<"GET">>]}], Req}.


%% content_types_provided/3
%% ====================================================================
%% @doc Will be called when processing a GET request.
%% Should return list of provided content-types, taking into account (if needed):
%%   - version
%%   - requested ID
%% Should return empty list if given request cannot be processed.
%% @end
-spec content_types_provided(req(), binary(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version, _Id) ->
    {[<<"application/json">>], Req}.


%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL. 
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, Id) ->
    try
        Filepath = binary_to_list(Id),
        {ok, _} = logical_files_manager:getfileattr(Filepath),
        {true, Req}
    catch _:_ ->
        {false, Req}
    end.


%% get/3
%% ====================================================================
%% @doc Will be called for GET requests. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec get(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
get(Req, <<"1.0">>, Id) ->
    case Id of
        undefined ->
            {{error, <<"dupa">>}, Req};
        _ ->
            try
                Filepath = binary_to_list(Id),
                {ok, Fileattr} = logical_files_manager:getfileattr(Filepath),
                MappedFileattr = map(Fileattr),
                Response = rest_utils:encode_to_json({struct, MappedFileattr}),
                {{ok, Response}, Req}
            catch
                _:_ ->
                    {{error, <<"cycki">>}, Req}
            end
    end.


%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec delete(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
delete(Req, <<"1.0">>, _Id) ->
    {error, Req}.


%% post/4
%% ====================================================================
%% @doc Will be called for POST request. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec post(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
post(Req, <<"1.0">>, _Id, _Data) ->
    {error, Req}.


%% put/4
%% ====================================================================
%% @doc Will be called for PUT request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec put(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
put(Req, <<"1.0">>, _Id, _Data) ->
    {false, Req}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% map/1
%% ====================================================================
%% @doc This function will map #fileattributes{} into json-ready proplist.
%% See rest_utils:map/2
%% @end
-spec map(#fileattributes{}) -> [{binary(), binary()}].
%% ==================================================================== 
map(Record) ->
    rest_utils:map(Record, [mode, uid, gid, atime, mtime, ctime, type, size, uname, gname]).