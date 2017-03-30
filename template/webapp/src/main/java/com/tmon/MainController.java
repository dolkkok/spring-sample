package com.tmon;

import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.List;


@Controller
public class MainController {
    protected Logger logger = Logger.getLogger(this.getClass());

    @RequestMapping(value="/greeting", method = RequestMethod.GET)
    public String greeting(ModelMap model,
                        @RequestParam(value="message", required=false, defaultValue="1") String message) throws Exception {
        model.addAttribute("message", message);
        return "greeting";
    }

    @RequestMapping("/home")
    public String home(ModelMap model) throws Exception {
        model.addAttribute("name", "my");
        return "home";
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    public ModelAndView list() {

        List<String> names = new ArrayList<String>();
        names.add("민아");
        names.add("박보검");
        names.add("조인성");
        names.add("James");
        names.add("Lucas");

        ModelAndView model = new ModelAndView();
        model.addObject("names", names);
        model.setViewName("list");
        return model;
    }

    @RequestMapping(value = "/hello", method = RequestMethod.GET)
    @ResponseBody
    public String hello() {
        return "Hello World 333";
    }

    @RequestMapping(value = "/hello.do", method = RequestMethod.GET)
    @ResponseBody
    public String hellodo() {
        return "Do Hello World";
    }

    @RequestMapping(value = "/*", method = RequestMethod.GET)
    public ResponseEntity<String> pageNotFound() {
        return new ResponseEntity<String>("Page not found", HttpStatus.BAD_REQUEST);
    }

    @RequestMapping(value = "/some", method = RequestMethod.GET)
    public ResponseEntity<String> some() {
        return new ResponseEntity<String>("good", HttpStatus.OK);
    }


    @RequestMapping(value = "/dologin", method = RequestMethod.POST)
    public String login(HttpSession session, ModelMap modelMap,
                        @RequestParam("email") String email,
                        @RequestParam("password") String password) throws Exception {


        if (password.equalsIgnoreCase("admin")) {
            modelMap.addAttribute("email", email);
            session.setAttribute("email", email);
            return "welcome";
        }

        return "redirect:/pages/login_error.html";
    }

    @RequestMapping(value = "/admin", method = RequestMethod.GET)
    public String admin(HttpSession session, ModelMap modelMap) {
        String email = (String)session.getAttribute("email");
        modelMap.addAttribute("email", email);
        return "admin";
    }
}

