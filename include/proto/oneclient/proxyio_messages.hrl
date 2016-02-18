%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Protocol for ProxyIO.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(PROXYIO_MESSAGES_HRL).
-define(PROXYIO_MESSAGES_HRL, 1).

-include("common_messages.hrl").

-record(remote_write, {
    offset :: non_neg_integer(),
    data :: binary()
}).

-record(remote_read, {
    offset :: non_neg_integer(),
    size :: pos_integer()
}).

-record(remote_data, {
    data :: binary()
}).

-record(remote_write_result, {
    wrote :: non_neg_integer()
}).

-type proxyio_request() :: #remote_read{} | #remote_write{}.
-type proxyio_response() :: #remote_data{} | #remote_write_result{}.

-record(proxyio_request, {
    parameters = #{} :: #{binary() => binary()},
    storage_id :: storage:id(),
    file_id :: helpers:file(),
    proxyio_request :: proxyio_request()
}).

-record(proxyio_response, {
    status :: #status{},
    proxyio_response :: proxyio_response()
}).

-endif.
