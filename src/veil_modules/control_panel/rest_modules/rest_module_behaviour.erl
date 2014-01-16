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
    Result :: [Fun_def]
    | undefined,
    Fun_def :: tuple(),
    Other :: any().
%% ====================================================================
behaviour_info(callbacks) ->
    [
        {methods_and_versions_info, 1},
        {exists, 3},
        {get, 3},
        {delete, 3}, % true | false
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
%% Another argument passed to every callback (besides methods_and_versions_info/1)
%% is version of API. It is a binary string denoting API version (e. g. <<"1.0">>)
%% When <<"latest">> is used in request, it will contain the highest available version.
%% Requested ID is passed to most of callbacks. It will contain binary representation of
%% ID that was found in requested URI, or 'undefined' for empty ID.


%% NOTE!
%% There is an optional callback - handle_multipart_data/4. It will be called 
%% only if a rest_module implements it. It is designed to handle multipart requests.
%% See description at the end of file


%% NOTE!
%% Returned values from get/put/delete/post callbacks:
%% Above callbacks should all return one of the following:
%% 1. {ok, Req} - upon success,
%%    when there is no content to return (code 204)
%%
%% 2. {{body, ResponseBody}, Req} - upon success,
%%    when there is some content to be sent back (code 200)
%%
%% 3. {{stream, Size, Fun, ContentType}, Req} - upon success,
%%    when large body has to be sent back by cowboy-like streaming function (code 200)
%%
%% 4. {error, Req} - upon failure,
%%    when no content should be returned (code 500)
%%
%% 5. {{error, ErrorDesc}, Req} - upon failure,
%%    when there is some content to be sent back (code 500)
%%
%% "application/json" will be assumed for any content returned as ResponseBody or ErrorDesc.
%% To return short reply describing operation success/failure, success_reply/1 and error_reply/1 can be used.
%% When returning large body with a streaming function, it is required to specify
%% ContentType (binary()), which will set response "content-type" header to desired.


%% methods_and_versions_info/1
%% ====================================================================
%% Function: methods_and_versions_info(req()) -> {[{binary(), [binary()]}], req()}.
%% Desription: Should return list of tuples, where each tuple consists of version of API version and
%% list of methods available in the API version. Latest version must be at the end of list.
%% e.g.: `[{<<"1.0">>, [<<"GET">>, <<"POST">>]}]'
%% ====================================================================


%% exists/3
%% ====================================================================
%% Function: exists(req(), binary(), binary()) -> {boolean(), req()}.
%% Desription: Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL. 
%% ====================================================================


%% get/3
%% ====================================================================
%% Function: get(req(), binary(), binary()) -> {Response, req()} when
%%     Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% Desription: Will be called for GET requests. Must return one of answers
%% described in rest_module_behaviour.
%% ====================================================================


%% delete/3
%% ====================================================================
%% Function: delete(req(), binary(), binary()) -> {Response, req()} when
%%     Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% Desription: Will be called for DELETE request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% ====================================================================


%% post/4
%% ====================================================================
%% Function: post(req(), binary(), binary(), term()) -> {Response, req()} when
%%     Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% Desription: Will be called for POST request. Must return one of answers
%% described in rest_module_behaviour.
%% ====================================================================


%% put/4
%% ====================================================================
%% Function: put(req(), binary(), binary(), term()) -> {Response, req()} when
%%     Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% Desription: Will be called for PUT request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% ====================================================================


%% handle_multipart_data/4
%% ====================================================================
%% Function: handle_multipart_data(req(), binary(), binary(), term()) -> {Response, req()} when
%%     Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% Desription: Optional callback to handle multipart requests. Data should be streamed
%% in handling module with use of cowboy_multipart module. Method can be `<<"POST">> or <<"PUT">>'.
%% Must return one of answers described in rest_module_behaviour.
%% ====================================================================