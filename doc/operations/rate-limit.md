# rate-limit.md
this upstream api return http code 400 with json {"error_message":429} for rate limit, worker should retry later when face this situation.