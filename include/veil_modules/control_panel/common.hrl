%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains common macros and records for control_panel modules
%% @end
%% ===================================================================

-ifndef(CONTROL_PANEL_COMMON_HRL).
-define(CONTROL_PANEL_COMMON_HRL, 1).

-include_lib("n2o/include/wf.hrl").
-include_lib("veil_modules/control_panel/custom_elements.hrl").
-include_lib("veil_modules/dao/common.hrl").

% Relative suffix of GUI address, leading to shared files
-define(shared_files_download_path, "/share/").

% Identifier for requests of shared files
-define(shared_files_request_type, shared_files).

% Relative suffix of GUI address, leading to user content download
-define(user_content_download_path, "/user_content").

% Identifier for requests of user content
-define(user_content_request_type, user_content).

% Relative suffix of GUI address, leading to file upload service
-define(file_upload_path, "/upload").

% Relative suffix of GUI address, leading to privacy policy page
-define(privacy_policy_url, <<"/privacy_policy">>).

% Macros used as ids of errors that can appear on GUI pages
-define(error_user_content_not_logged_in, uc_not_logged_in).
-define(error_user_content_file_not_found, uc_file_not_found).
-define(error_shared_file_not_found, sh_file_not_found).
-define(error_internal_server_error, internal_server_error).
-define(error_openid_invalid_request, openid_invalid_request).
-define(error_openid_auth_invalid, openid_auth_invalid).
-define(error_openid_no_connection, openid_no_connection).
-define(error_openid_login_error, openid_login_error).
-define(error_login_dir_creation_error, login_dir_creation_error).
-define(error_login_dir_chown_error, login_dir_chown_error).


%% Includes from cowboy
-type cookie_option() :: {max_age, non_neg_integer()}
	| {domain, binary()} | {path, binary()}
	| {secure, boolean()} | {http_only, boolean()}.
-type cookie_opts() :: [cookie_option()].
-export_type([cookie_opts/0]).

-type content_decode_fun() :: fun((binary())
	-> {ok, binary()}
	| {error, atom()}).
-type transfer_decode_fun() :: fun((binary(), any())
	-> {ok, binary(), binary(), any()}
	| more | {more, non_neg_integer(), binary(), any()}
	| {done, non_neg_integer(), binary()}
	| {done, binary(), non_neg_integer(), binary()}
	| {error, atom()}).

-type resp_body_fun() :: fun((any(), module()) -> ok).
-type send_chunk_fun() :: fun((iodata()) -> ok | {error, atom()}).
-type resp_chunked_fun() :: fun((send_chunk_fun()) -> ok).

-record(http_req, {
	%% Transport.
	socket = undefined :: any(),
	transport = undefined :: undefined | module(),
	connection = keepalive :: keepalive | close,

	%% Request.
	pid = undefined :: pid(),
	method = <<"GET">> :: binary(),
	version = 'HTTP/1.1' :: cowboy:http_version(),
	peer = undefined :: undefined | {inet:ip_address(), inet:port_number()},
	host = undefined :: undefined | binary(),
	host_info = undefined :: undefined | cowboy_router:tokens(),
	port = undefined :: undefined | inet:port_number(),
	path = undefined :: binary(),
	path_info = undefined :: undefined | cowboy_router:tokens(),
	qs = undefined :: binary(),
	qs_vals = undefined :: undefined | list({binary(), binary() | true}),
	bindings = undefined :: undefined | cowboy_router:bindings(),
	headers = [] :: cowboy:http_headers(),
	p_headers = [] :: [any()],
	cookies = undefined :: undefined | [{binary(), binary()}],
	meta = [] :: [{atom(), any()}],

	%% Request body.
	body_state = waiting :: waiting | done | {stream, non_neg_integer(),
		transfer_decode_fun(), any(), content_decode_fun()},
	multipart = undefined :: undefined | {non_neg_integer(), fun()},
	buffer = <<>> :: binary(),

	%% Response.
	resp_compress = false :: boolean(),
	resp_state = waiting :: locked | waiting | chunks | done,
	resp_headers = [] :: cowboy:http_headers(),
	resp_body = <<>> :: iodata() | resp_body_fun()
		| {non_neg_integer(), resp_body_fun()}
		| {chunked, resp_chunked_fun()},

	%% Functions.
	onresponse = undefined :: undefined | already_called
		| cowboy:onresponse_fun()
}).

-opaque req() :: #http_req{}.
-export_type([req/0]).


-endif.

