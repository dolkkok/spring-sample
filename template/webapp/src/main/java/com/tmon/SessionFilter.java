package com.tmon;

import org.apache.log4j.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class SessionFilter implements Filter {

    private FilterConfig filterConfig;
    protected Logger logger = Logger.getLogger(this.getClass());

    public SessionFilter(){
    }

    public void destroy(){
        filterConfig = null;
    }

    public void init(FilterConfig filterconfig) throws ServletException{
        filterConfig = filterconfig;
    }

    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws java.io.IOException, javax.servlet.ServletException {
        HttpServletResponse res = (HttpServletResponse)servletResponse;
        HttpServletRequest req = (HttpServletRequest)servletRequest;
        HttpSession httpsession = req.getSession();

        String uri = req.getRequestURI();
        logger.info(uri);

        /// 로그인이 필요한 페이지라면
        if (uri.equalsIgnoreCase("/admin")) {
            String email = (String)httpsession.getAttribute("email");

            if (email == null) {
                res.sendRedirect("/pages/login.html");
                return;
            }
           // req.getRequestDispatcher("/").forward(req, res);
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }
}