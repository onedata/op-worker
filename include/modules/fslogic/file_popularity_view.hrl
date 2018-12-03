%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Macros and records used by file_popularity_view module
%%% @end
%%%-------------------------------------------------------------------

-define(VIEW_NAME(SpaceId), <<"file-popularity-", SpaceId/binary>>).

-record(token, {
    last_doc_id :: undefined | binary(),
    start_key :: binary(),
    end_key :: binary()
}).