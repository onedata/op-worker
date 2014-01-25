%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements rest_module_behaviour and handles all
%% REST requests directed at /rest/attrs/(path). It returns file attributes,
%% if possible.
%% @end
%% ===================================================================

-module(rest_attrs).
-behaviour(rest_module_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/control_panel/rest_utils.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

-export([allowed_methods/3, content_types_provided/2, content_types_provided/3]).
-export([exists/3, get/2, get/3, delete/3, validate/4, post/4, put/4]).


%% ====================================================================
%% Behaviour callback functions
%% ====================================================================

%% allowed_methods/3
%% ====================================================================
%% @doc Should return list of methods that are allowed and directed at specific Id.
%% e.g.: if Id =:= undefined -> `[<<"GET">>, <<"POST">>]'
%%       if Id  /= undefined -> `[<<"GET">>, <<"PUT">>, <<"DELETE">>]'
%% @end
-spec allowed_methods(req(), binary(), binary()) -> {[binary()], req()}.
%% ====================================================================
allowed_methods(Req, _Version, Id) ->
    Answer = case Id of
        undefined -> [];
        _ -> [<<"GET">>]
    end,
    {Answer, Req}.
    

%% content_types_provided/2
%% ====================================================================
%% @doc Should return list of provided content-types
%% without specified ID (e.g. ".../rest/resource/"). 
%% Should take into account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%% @end
-spec content_types_provided(req(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version) ->
    {[], Req}.


%% content_types_provided/3
%% ====================================================================
%% @doc Should return list of provided content-types
%% with specified ID (e.g. ".../rest/resource/some_id"). Should take into
%% account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%% @end
-spec content_types_provided(req(), binary(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version, _Id) ->
    {Method, _} = cowboy_req:method(Req),
    Answer = case Method of
        <<"GET">> -> [<<"application/json">>];
        _ -> []
    end,
    {Answer, Req}.


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


%% get/2
%% ====================================================================
%% @doc Will be called for GET request without specified ID 
%% (e.g. ".../rest/resource/"). Should return one of the following:
%% 1. ResponseBody, of the same type as content_types_provided/1 returned 
%%    for this request
%% 2. Cowboy type stream function, serving content of the same type as 
%%    content_types_provided/1 returned for this request
%% 3. 'halt' atom if method is not supported
%% @end
-spec get(req(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}.
%% ====================================================================
get(Req, _Version) -> 
    {halt, Req}.


%% get/3
%% ====================================================================
%% @doc Will be called for GET request with specified ID
%% (e.g. ".../rest/resource/some_id"). Should return one of the following:
%% 1. ResponseBody, of the same type as content_types_provided/2 returned 
%%    for this request
%% 2. Cowboy type stream function, serving content of the same type as 
%%    content_types_provided/2 returned for this request
%% 3. 'halt' atom if method is not supported
%% @end
-spec get(req(), binary(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}.
%% ====================================================================
get(Req, _Version, Id) -> 
    try
        Filepath = binary_to_list(Id),
        {ok, Fileattr} = logical_files_manager:getfileattr(Filepath),
        MappedFileattr = map(Fileattr),
        Response = rest_utils:encode_to_json({struct, MappedFileattr}), 
        {Response, Req}
    catch _:_ ->
        {halt, Req}
    end.


%% validate/4
%% ====================================================================
%% @doc Should return true/false depending on whether the request is valid
%% in terms of the handling module. Will be called before POST or PUT,
%% should discard unprocessable requests.
%% @end
-spec validate(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
validate(Req, _Version, _Id, _Data) -> 
    {false, Req}.


%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Should try to remove 
%% specified resource and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% @end
-spec delete(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
delete(Req, _Version, _Id) -> 
    {false, Req}.


%% post/4
%% ====================================================================
%% @doc Will be called for POST request, after the request has been validated. 
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% Returning {true, URL} will cause the reply to contain 201 redirect to given URL.
%% @end
-spec post(req(), binary(), binary(), term()) -> {boolean() | {true, binary()}, req()}.
%% ====================================================================
post(Req, _Version, _Id, _Data) -> 
    {false, Req}.


%% put/4
%% ====================================================================
%% @doc Will be called for PUT request on given ID, after the request has been validated. 
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% @end
-spec put(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
put(Req, _Version, _Id, _Data) -> 
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
    rest_utils:map(Record, [mode, uid, gid, atime, mtime, ctime, type, size, uname, gname, links]).
