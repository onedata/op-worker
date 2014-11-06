%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This behaviour defines API for a DNS query handler.
%% @end
%% ===================================================================
-module(dns_query_handler_behaviour).


%% handle_xxx/1
%% ====================================================================
%% @doc Callbacks below handle specific types od DNS queries, in accordance to RFC1035:
%% {@link https://tools.ietf.org/html/rfc1035#section-3.2.2}
%% The argument in every function is Domain that was queried for, as a loewr-case string.
%% @end
%% ====================================================================
-callback handle_a(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()}.
    % Returns IPv4 address(es) - IP address(es) of servers: (RFC1035 3.4.1)
    % {A, B, C, D}: Bytes of IPv4 address

-callback handle_ns(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: string().
    % Returns hostname(s): (RFC1035 3.3.11)
    % A <domain-name> which specifies a host which should be authoritative for the specified class and domain.

-callback handle_cname(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: string().
    % Returns hostname(s): (RFC1035 3.3.1)
    % A <domain-name> which specifies the canonical or primary name for the owner.  The owner name is an alias.

-callback handle_soa(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {MName :: string() , RName:: string(), Serial :: integer(), Refresh :: integer(),
    Retry :: integer(), Expiry :: integer(), Minimum :: integer()}.
    % Returns: (RFC1035 3.3.13)
    % MName: The <domain-name> of the name server that was the original or primary source of data for this zone.
    % RName: A <domain-name> which specifies the mailbox of the person responsible for this zone.
    % Serial: The unsigned 32 bit version number of the original copy of the zone.  Zone transfers preserve this value.
    %    This value wraps and should be compared using sequence space arithmetic.
    % Refresh: A 32 bit time interval before the zone should be refreshed.
    % Retry: A 32 bit time interval that should elapse before a failed refresh should be retried.
    % Expiry: A 32 bit time value that specifies the upper limit on the time interval that can elapse before the zone
    %   is no longer authoritative.
    % Minimum: The unsigned 32 bit minimum TTL field that should be exported with any RR from this zone.

-callback handle_wks(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {{A :: byte(), B :: byte(), C :: byte(), D :: byte()}, Proto :: string(), BitMap :: string()}.
    % Returns: (RFC1035 3.4.2)
    % {A, B, C, D}: Bytes of IPv4 address
    % Proto: An 8 bit IP protocol number
    % BitMap: A variable length bit map. The bit map must be a multiple of 8 bits long.
    %   A positive bit means, that a port of number equal to its position in bitmap is open.
    %   BitMap representation as string in erlang is problematic. Example of usage:
    %   BitMap = [2#11111111, 2#00000000, 2#00000011]
    %   above bitmap will inform the client, that ports: 0 1 2 3 4 5 6 7 22 23 are open.

-callback handle_ptr(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: string().
    % Returns hostname(s): (RFC1035 3.3.12)
    % <domain-name> which points to some location in the domain name space.

-callback handle_hinfo(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {CPU :: string(), OS :: string()}.
    % Returns: (RFC1035 3.3.2)
    % CPU: A <character-string> which specifies the CPU type.
    % OS: A <character-string> which specifies the operatings system type.

-callback handle_minfo(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {RM :: string(), EM :: string()}.
    % Returns: (RFC1035 3.3.7)
    % RM: A <domain-name> which specifies a mailbox which is responsible for the mailing list or mailbox.  If this
    %   domain name names the root, the owner of the MINFO RR is responsible for itself.  Note that many existing mailing
    %   lists use a mailbox X-request for the RMAILBX field of mailing list X, e.g., Msgroup-request for Msgroup.  This
    %   field provides a more general mechanism.
    % EM: A <domain-name> which specifies a mailbox which is to receive error messages related to the mailing list or
    %   mailbox specified by the owner of the MINFO RR (similar to the ERRORS-TO: field which has been proposed).  If
    %   this domain name names the root, errors should be returned to the sender of the message.

-callback handle_mx(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: {Pref :: integer(), Exch :: string()}.
    % Returns: (RFC1035 3.3.9)
    % Pref: A 16 bit integer which specifies the preference given to this RR among others at the same owner.
    %   Lower values are preferred.
    % Exch: A <domain-name> which specifies a host willing to act as a mail exchange for the owner name.

-callback handle_txt(Domain :: string()) -> {ok, [Response]} | serv_fail | nx_domain | not_impl | refused
    when Response :: [string()].
    % Returns text data - one or more <character-string>s: (RFC1035 3.3.14)
    % TXT RRs are used to hold descriptive text. The semantics of the text depends on the domain where it is found.
    % NOTE: Should return list of strings for every record, for example:
    % [["string_1_1", "string_1_2"], ["string_2_1", "string_2_2"]] - two records, each with two strings.



