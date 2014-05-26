%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements rest_module_behaviour and handles all
%% REST requests directed at /rest/files/(path). Essentially, it serves 
%% user content (files) via HTTP.
%% @end
%% ===================================================================

-module(rest_files).
-behaviour(rest_module_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/control_panel/rest_messages.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("err.hrl").


-export([methods_and_versions_info/1, exists/3]).
-export([get/3, delete/3, post/4, put/4]).
% optional callback
-export([handle_multipart_data/4]).


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
    {[{<<"1.0">>, [<<"GET">>, <<"PUT">>, <<"POST">>, <<"DELETE">>]}], Req}.


%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL (is NOT undefined).
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, Id) ->
    Filepath = binary_to_list(Id),
    Answer = case logical_files_manager:getfileattr(Filepath) of
                 {ok, Attr} ->
                     % File exists, cache retireved info so as not to ask DB later
                     case Attr#fileattributes.type of
                         "REG" ->
                             % Remember that path is a file
                             erlang:put(file_type, reg),
                             % Remember file attrs
                             erlang:put(file_attr, Attr);
                         "DIR" ->
                             % Remember that path is a dir
                             erlang:put(file_type, dir)
                     end,
                     true;
                 _ ->
                     % getfileattr returned error, resource does not exist
                     false
             end,
    {Answer, Req}.


%% get/3
%% ====================================================================
%% @doc Will be called for GET requests. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec get(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
get(Req, <<"1.0">>, Id) ->
    FilePath = case Id of
                   undefined ->
                       % Empty ID lists user's root, so it's a dir
                       erlang:put(file_type, dir),
                       "/";
                   Path ->
                       binary_to_list(Path)
               end,
    case erlang:get(file_type) of
        dir ->
            {list_dir_to_json(FilePath), Req};
        reg ->
            % File attrs were cached in exists/3
            Fileattr = erlang:get(file_attr),
            Size = Fileattr#fileattributes.size,
            StreamFun = file_download_handler:cowboy_file_stream_fun(FilePath, Size),
            NewReq = file_download_handler:content_disposition_attachment_headers(Req, filename:basename(FilePath)),
            [Mimetype] = mimetypes:path_to_mimes(FilePath),
            {{stream, Size, StreamFun, Mimetype}, NewReq}
    end.


%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec delete(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
delete(Req, <<"1.0">>, Id) ->
    Filepath = binary_to_list(Id),
    Response = case erlang:get(file_type) of
                   dir ->
                       ErrorRec = ?report_warning(?error_dir_cannot_delete),
                       {error, rest_utils:error_reply(ErrorRec)};
                   reg ->
                       case logical_files_manager:delete(Filepath) of
                           ok ->
                               {body, rest_utils:success_reply(?success_file_deleted)};
                           _ ->
                               ErrorRec = ?report_error(?error_reg_file_cannot_delete, [Filepath]),
                               {error, rest_utils:error_reply(ErrorRec)}
                       end
               end,
    {Response, Req}.


%% post/4
%% ====================================================================
%% @doc Will be called for POST request. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec post(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
post(Req, <<"1.0">>, _Id, _Data) ->
    % Return 422 unprocessable, because no "mulitpart/form-data" header was found
    % otherwise handle_multipart_data/4 would be called
    ErrorRec = ?report_error(?error_upload_unprocessable),
    {{error, rest_utils:error_reply(ErrorRec)}, Req}.


%% put/4
%% ====================================================================
%% @doc Will be called for PUT request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec put(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
put(Req, <<"1.0">>, _Id, _Data) ->
    % Return 422 unprocessable, because no "mulitpart/form-data" header was found
    % otherwise handle_multipart_data/4 would be called
    ErrorRec = ?report_error(?error_upload_unprocessable),
    {{error, rest_utils:error_reply(ErrorRec)}, Req}.


%% handle_multipart_data/4
%% ====================================================================
%% @doc Optional callback to handle multipart requests. Data should be streamed
%% in handling module with use of cowboy_multipart module. Method can be `<<"POST">> or <<"PUT">>'.
%% Must return one of answers described in rest_module_behaviour.
%% @end
-spec handle_multipart_data(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
handle_multipart_data(Req, _Version, Method, Id) ->
    case Method of
        <<"POST">> -> file_upload_handler:handle_rest_upload(Req, Id, false);
        <<"PUT">> -> file_upload_handler:handle_rest_upload(Req, Id, true)
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% list_dir_to_json/1
%% ====================================================================
%% @doc Lists the directory and returns response in JSON (content or error).
%% @end
-spec list_dir_to_json(string()) -> {body, binary()} | {error, binary()}.
%% ====================================================================
list_dir_to_json(Path) ->
    case list_dir(Path) of
        {error, not_a_dir} ->
            {error, <<"error: not a dir">>};
        DirList ->
            DirListBin = lists:map(
                fun(Dir) ->
                    list_to_binary(Dir)
                end, DirList),
            Body = {array, DirListBin},
            {body, rest_utils:encode_to_json(Body)}
    end.


%% list_dir/1
%% ====================================================================
%% @doc List the given directory, calling itself recursively if there is more to fetch.
%% @end
-spec list_dir(string()) -> [string()].
%% ====================================================================
list_dir(Path) ->
    list_dir(Path, 0, 10, []).

list_dir(Path, Offset, Count, Result) ->
    case logical_files_manager:ls(Path, Count, Offset) of
        {ok, FileList} ->
            case length(FileList) of
                Count -> list_dir(Path, Offset + Count, Count * 10, Result ++ FileList);
                _ -> Result ++ FileList
            end;
        _ ->
            {error, not_a_dir}
    end.