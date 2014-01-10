%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This behaviour should be implemented by modules handling REST requests.
%% It ensures the presence of required callbacks.
%% @end
%% ===================================================================

-module(rest_module_behaviour).

-export([behaviour_info/1]).

%% behaviour_info/1
%% ====================================================================
%% @doc Defines the behaviour (lists the callbacks and their arity)
-spec behaviour_info(Arg) -> Result when
	Arg :: callbacks | Other,
	Result ::  [Fun_def] 
			| undefined,
	Fun_def :: tuple(),
	Other :: any().
%% ====================================================================
behaviour_info(callbacks) -> 
	[
		{allowed_methods, 3},
		{content_types_provided, 2},
		{content_types_provided, 3},
	    {exists, 3},
	    {get, 2},
	    {get, 3},
	    {delete, 3}, % true | false
		{validate, 4},
	    {post, 4}, % true | false
	    {put, 4} % true | false
	];

behaviour_info(_Other) ->
    undefined.

%% ====================================================================
%% Callbacks descriptions
%% ====================================================================

%% NOTE!
%% All callbacks take cowboy #http_req{} record as an argument. This is
%% so that the callbacks can use or alter the request.
%% The request record must be returned from every callback, and if it
%% has been altered, the changed version should be returned for
%% the modifications to persist.
%% Another argument passed to every callback is version of API. This is
%% either string denoting API version (e. g. <<"1.0">>) or keyword 
%% <<"latest">> which should be interpreted as highest available version.


%% NOTE!
%% There is an optional callback - handle_multipart_data/4. It will be called 
%% only if a rest_module implements it. It is designed to handle multipart requests.
%% See description at the end of file


%% allowed_methods/3
%% ====================================================================
%% Function: allowed_methods(req(), binary(), binary()) -> {[binary()], req()}.
%% Desription: Should return list of methods that are allowed and directed at specific Id.
%% e.g.: if Id =:= undefined -> `[<<"GET">>, <<"POST">>]'
%%       if Id  /= undefined -> `[<<"GET">>, <<"PUT">>, <<"DELETE">>]'
%% ====================================================================


%% content_types_provided/2
%% ====================================================================
%% Function: content_types_provided(req(), binary()) -> {[binary()], req()}.
%% Desription: Should return list of provided content-types
%% without specified ID (e.g. ".../rest/resource/"). 
%% Should take into account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%% ====================================================================


%% content_types_provided/3
%% ====================================================================
%% Function: content_types_provided(req(), binary(), binary()) -> {[binary()], req()}.
%% Desription: Should return list of provided content-types
%% with specified ID (e.g. ".../rest/resource/some_id"). Should take into
%% account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%% ====================================================================


%% exists/3
%% ====================================================================
%% Function: exists(req(), binary(), binary()) -> {boolean(), req()}.
%% Desription: Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL. 
%% ====================================================================


%% get/2
%% ====================================================================
%% Function: get(req(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}.
%% Desription: Will be called for GET request without specified ID 
%% (e.g. ".../rest/resource/"). Should return one of the following:
%% 1. ResponseBody, of the same type as content_types_provided/1 returned 
%%    for this request
%% 2. Cowboy type stream function, serving content of the same type as 
%%    content_types_provided/1 returned for this request
%% 3. 'halt' atom if method is not supported
%% ====================================================================

%% get/3
%% ====================================================================
%% Function: get(req(), binary(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}.
%% Desription: Will be called for GET request with specified ID
%% (e.g. ".../rest/resource/some_id"). Should return one of the following:
%% 1. ResponseBody, of the same type as content_types_provided/2 returned 
%%    for this request
%% 2. Cowboy type stream function, serving content of the same type as 
%%    content_types_provided/2 returned for this request
%% 3. 'halt' atom if method is not supported
%% ====================================================================


%% validate/4
%% ====================================================================
%% Function: validate(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% Desription: Should return true/false depending on whether the request is valid
%% in terms of the handling module. Will be called before POST or PUT,
%% should discard unprocessable requests.
%% ====================================================================


%% delete/3
%% ====================================================================
%% Function: delete(req(), binary(), binary()) -> {boolean(), req()}.
%% Desription: Will be called for DELETE request on given ID. Should try to remove 
%% specified resource and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% ====================================================================


%% post/4
%% ====================================================================
%% Function: post(req(), binary(), binary(), term()) -> {boolean() | {true, binary()}, req()}.
%% Desription: Will be called for POST request, after the request has been validated. 
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% Returning {true, URL} will cause the reply to contain 201 redirect to given URL.
%% ====================================================================


%% put/4
%% ====================================================================
%% Function: put(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% Desription: Will be called for PUT request on given ID, after the request has been validated. 
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% ====================================================================


%% handle_multipart_data/4
%% ====================================================================
%% Function: handle_multipart_data(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% Desription: Optional callback to handle multipart requests. Data should be streamed
%% in handling module with use of cowboy_multipart module. Method can be `<<"POST">> or <<"PUT">>'.
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% ====================================================================