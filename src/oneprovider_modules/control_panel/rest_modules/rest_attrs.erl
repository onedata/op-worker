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

-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/control_panel/rest_messages.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("err.hrl").


-export([methods_and_versions_info/1, exists/3]).
-export([get/3, delete/3, post/4, put/4]).


%% ====================================================================
%% Behaviour callback functions
%% ====================================================================


%% methods_and_versions_info/1
%% ====================================================================
%% @doc Should return list of tuples, where each tuple consists of version of API version and
%% list of methods available in the API version. Latest version must be at the end of list.
%% e.g.: `[{<<"1.0">>, [<<"GET">>, <<"POST">>]}]'
%% @end
-spec methods_and_versions_info(req()) -> {[{binary(), [binary()]}], req()}.
%% ====================================================================
methods_and_versions_info(Req) ->
    {[{<<"1.0">>, [<<"GET">>]}], Req}.


%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL. 
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, Id) ->
    try
        Filepath = gui_str:binary_to_unicode_list(Id),
        {ok, Attr} = logical_files_manager:getfileattr(Filepath),
        % Remember file attrs for later use
        erlang:put(file_attr, Attr),
        {true, Req}
    catch
        _:_ ->
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
            ErrorRec = ?report_warning(?error_no_id_in_uri),
            {{error, rest_utils:error_reply(ErrorRec)}, Req};
        _ ->
            Attr = erlang:get(file_attr),
            MappedFileattr = map(Attr),
            Response = rest_utils:encode_to_json({struct, MappedFileattr}),
            {{body, Response}, Req}
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
    {error, Req}.


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
map(FileAttrs) ->
    #fileattributes{mode = Mode, uid = Uid, gid = Gid, atime = Atime, mtime = Mtime, ctime = Ctime,
        type = Type, size = Size, uname = Uname, gname = Gname, links = Links} = FileAttrs,
    [
        {<<"mode">>, Mode},
        {<<"uid">>, Uid},
        {<<"gid">>, Gid},
        {<<"atime">>, Atime},
        {<<"mtime">>, Mtime},
        {<<"ctime">>, Ctime},
        {<<"type">>, Type},
        {<<"size">>, Size},
        {<<"uname">>, gui_str:unicode_list_to_binary(Uname)},
        {<<"gname">>, gui_str:unicode_list_to_binary(Gname)},
        {<<"links">>, Links}
    ].
